"""Theoretical best/expected/worst-case resource estimates, per component.

Produces a component-by-component RAM and disk estimate from first principles
(dataset size, dimension, and the instance's tuning parameters), so readers can
compare predicted numbers against the measured results in the same workbook.

Scenario definitions:
- Best:     minimal steady-state footprint, no overhead (lower bound).
- Expected: typical footprint including realistic format/service overhead.
- Worst:    peak during indexing/segment optimization (capacity planning bound;
            follows Qdrant's guidance of ~1.5x raw vector size for RAM and
            temporary segment copies on disk).

All numbers are estimates for an HNSW-based engine (formulas are tuned for
Qdrant's defaults but are representative for similar architectures).
"""

from __future__ import annotations

_BYTES_PER_ELEM = {"float32": 4, "float16": 2, "uint8": 1}
_PQ_RATIO = {"x4": 4, "x8": 8, "x16": 16, "x32": 32, "x64": 64}

# Fixed service overhead (runtime, buffers, allocator) in bytes: best/expected/worst.
_SERVICE_OVERHEAD_RAM = (30 * 1024**2, 100 * 1024**2, 200 * 1024**2)


def _flag(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"


def _to_int(v, default: int) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def compute_theoretical_breakdown(n: int, dim: int, tuning: dict) -> dict | None:
    """Return {'components': [...], 'totals': {...}, 'assumptions': [...]} or None if n/dim unknown.

    Each component is a dict:
        {"component", "ram": (best, expected, worst), "disk": (best, expected, worst), "note"}
    """
    if not n or not dim:
        return None
    tuning = tuning or {}

    datatype = str(tuning.get("vector_datatype", "float32")).strip().lower()
    bpe = _BYTES_PER_ELEM.get(datatype, 4)
    m = _to_int(tuning.get("m") or tuning.get("M"), 16)
    on_disk_vectors = _flag(tuning.get("on_disk", False))
    hnsw_on_disk = _flag(tuning.get("hnsw_on_disk", False))
    on_disk_payload = _flag(tuning.get("on_disk_payload", True))
    quant_mode = str(tuning.get("quantization_mode", "none")).strip().lower()
    wal_capacity_mb = _to_int(tuning.get("wal_capacity_mb"), 32)
    wal_segments_ahead = _to_int(tuning.get("wal_segments_ahead"), 0)

    raw = n * dim * bpe
    components: list[dict] = []

    # --- Original vectors ---
    vec_disk = (raw, int(raw * 1.05), raw * 2)  # worst: temp segment copies during optimization
    if on_disk_vectors:
        vec_ram = (0, int(raw * 0.1), raw)  # on disk: only hot pages cached; worst = fully cached
        vec_note = f"{n:,} x {dim} x {bpe} B ({datatype}). on_disk=true: served from disk/page cache."
    else:
        vec_ram = (raw, int(raw * 1.1), int(raw * 1.5))
        vec_note = (
            f"{n:,} x {dim} x {bpe} B ({datatype}). Resident for full performance; worst uses "
            "Qdrant's ~1.5x planning factor. NOTE: memory-mapped, so this shows up as OS page "
            "cache (Cached), not container RSS in avg/peak memory usage."
        )
    components.append({"component": "Original vectors", "ram": vec_ram, "disk": vec_disk, "note": vec_note})

    # --- HNSW graph ---
    links0 = n * 2 * m * 4  # layer 0: 2*m links x 4 B each
    graph = (links0, int(links0 * 1.1), int(links0 * 1.5))  # +~10% upper layers; worst: rebuild overhead
    graph_ram = (0, int(links0 * 0.1), links0) if hnsw_on_disk else graph
    components.append({
        "component": "HNSW index (graph links)",
        "ram": graph_ram,
        "disk": (links0, int(links0 * 1.1), links0 * 2),
        "note": f"n x 2m x 4 B = {n:,} x {2 * m} x 4 B (layer 0) + ~10% upper layers; m={m}.",
    })

    # --- Quantized vectors ---
    if quant_mode and quant_mode != "none":
        if quant_mode == "scalar":
            q = n * dim * 1 + n * 8  # int8 + per-vector scale/offset
            q_note = f"scalar int8: {n:,} x {dim} x 1 B + 8 B/vector correction factors."
            always_ram = _flag(tuning.get("sq_always_ram", False))
        elif quant_mode == "product":
            ratio = _PQ_RATIO.get(str(tuning.get("pq_compression", "x16")).strip().lower(), 16)
            q = int(n * dim * 4 / ratio) + 256 * dim * 4  # codes + codebooks
            q_note = f"PQ {tuning.get('pq_compression', 'x16')}: raw float32 size / {ratio} + codebooks."
            always_ram = _flag(tuning.get("pq_always_ram", False))
        else:  # binary
            q = n * dim // 8
            q_note = f"binary: {n:,} x {dim} / 8 bits."
            always_ram = _flag(tuning.get("bq_always_ram", False))
        q_ram = (q, int(q * 1.1), int(q * 1.3)) if always_ram else (0, int(q * 0.5), q)
        components.append({
            "component": f"Quantized vectors ({quant_mode})",
            "ram": q_ram,
            "disk": (q, int(q * 1.05), int(q * 1.5)),
            "note": q_note + (" always_ram=true." if always_ram else " always_ram=false: page-cache resident."),
        })

    # --- Payload storage (pk integer per point) ---
    pay = (n * 20, n * 50, n * 120)  # key/value + storage-engine overhead per point
    pay_ram = (0, int(n * 5), n * 20) if on_disk_payload else pay
    components.append({
        "component": "Payload storage",
        "ram": pay_ram,
        "disk": pay,
        "note": "One integer field (pk) per point; 20-120 B/point incl. storage-engine overhead. "
        + ("on_disk_payload=true." if on_disk_payload else "on_disk_payload=false (in RAM)."),
    })

    # --- Payload index (integer index on pk) ---
    pidx = (n * 16, n * 24, n * 48)
    components.append({
        "component": "Payload index (pk)",
        "ram": pidx,
        "disk": pidx,
        "note": "Integer field index: ~16-48 B/point (kept in RAM).",
    })

    # --- ID tracker / point metadata ---
    idt = (n * 16, n * 24, n * 40)
    components.append({
        "component": "ID tracker & versions",
        "ram": idt,
        "disk": idt,
        "note": "External<->internal id maps + per-point versions: ~16-40 B/point.",
    })

    # --- WAL (disk only) ---
    wal_unit = wal_capacity_mb * 1024**2
    wal = (wal_unit, wal_unit * (1 + wal_segments_ahead), wal_unit * (2 + wal_segments_ahead))
    components.append({
        "component": "WAL",
        "ram": (0, 0, 0),
        "disk": wal,
        "note": f"wal_capacity_mb={wal_capacity_mb}, segments_ahead={wal_segments_ahead}; disk only.",
    })

    # --- Service overhead (RAM only) ---
    components.append({
        "component": "Service overhead",
        "ram": _SERVICE_OVERHEAD_RAM,
        "disk": (0, 0, 0),
        "note": "Runtime, connection buffers, allocator overhead (rough constant).",
    })

    totals = {
        "ram": tuple(sum(c["ram"][i] for c in components) for i in range(3)),
        "disk": tuple(sum(c["disk"][i] for c in components) for i in range(3)),
    }
    assumptions = [
        f"Dataset: {n:,} vectors x {dim} dims, {datatype} ({bpe} B/element); raw vector data = {raw:,} bytes.",
        "Best = minimal steady-state (lower bound). Expected = typical with format/service overhead. "
        "Worst = peak during indexing/optimization (capacity planning).",
        "RAM totals count all data that should be memory-resident for full performance. The benchmark's "
        "avg/peak memory usage metric (docker stats) EXCLUDES OS page cache, so memory-mapped components "
        "(vectors, quantized vectors) appear in the Component Usage sheet as 'Cached', not in those metrics.",
        "Formulas are tuned for Qdrant HNSW defaults; treat as order-of-magnitude estimates for other engines.",
    ]
    return {"components": components, "totals": totals, "assumptions": assumptions}
