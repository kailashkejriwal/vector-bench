import requests
import streamlit as st
import streamlit.components.v1 as components

from vectordb_bench.frontend.components.get_results.export_excel import build_results_excel

HTML_2_CANVAS_URL = "https://unpkg.com/html2canvas@1.4.1/dist/html2canvas.js"
JS_PDF_URL = "https://unpkg.com/jspdf@2.5.1/dist/jspdf.umd.min.js"


def _hash_shown_data(L):
    """Hashable representation of shown_data for cache key (uses metricsSet + values)."""
    if not L:
        return ()
    out = []
    for d in L:
        keys = sorted(d.get("metricsSet") or [])
        # Use repr for values so lists/dicts don't break hashing
        vals = tuple((k, repr(d.get(k))) for k in keys)
        out.append((d.get("bar_display_name"), d.get("case_name"), d.get("db"), vals))
    return tuple(out)


def _hash_failed_tasks(D):
    """Hashable representation of failed_tasks for cache key."""
    if not D:
        return ()
    return tuple(sorted((k, tuple(sorted(v.items()))) for k, v in D.items()))


@st.cache_data(ttl=600, hash_funcs={list: _hash_shown_data, dict: _hash_failed_tasks})
def _get_excel_bytes_cached(shown_data: list, failed_tasks: dict) -> bytes:
    """Cached Excel build so the same bytes object is reused across reruns (fixes 'File wasn't available on site')."""
    return build_results_excel(shown_data, failed_tasks or {})


@st.cache_data
def load_unpkg(src: str) -> str:
    return requests.get(src).text


def getResults(container, pageName="vectordb_bench", results_export_data=None):
    """
    Render "Get results" section with Save as Image, Save as PDF, and optionally Download Excel.

    results_export_data: optional tuple (shown_data, failed_tasks, selected_labels) from the results page.
        When provided, a "Download Excel" button is shown that exports the same data as the results page.
        selected_labels is used for the filename: Vector_Bench_<Label>.xlsx
    """
    container.subheader("Get results")
    if results_export_data is not None:
        saveAsExcel(container, results_export_data, pageName)
    saveAsImage(container, pageName)
    saveAsPDF(container, pageName)


def _safe_download_filename(label: str) -> str:
    """Build a safe filename segment from the selected run label."""
    if not label or not str(label).strip():
        return "results"
    safe = "".join(c if c.isalnum() or c in " ._-" else "_" for c in str(label).strip())
    return safe[:80].strip() or "results"


def saveAsExcel(container, results_export_data, pageName="vectordb_bench"):
    """Offer download of results as a formatted Excel file. Filename: Vector_Bench_<Label>.xlsx"""
    if len(results_export_data) >= 3:
        shown_data, failed_tasks, selected_labels = results_export_data[0], results_export_data[1], results_export_data[2]
    else:
        shown_data, failed_tasks = results_export_data[0], results_export_data[1]
        selected_labels = []
    if not shown_data and not failed_tasks:
        return
    try:
        excel_bytes = _get_excel_bytes_cached(shown_data, failed_tasks or {})
    except Exception as e:
        container.error(f"Excel export failed: {e}")
        return
    if not excel_bytes:
        return
    label = (selected_labels[0] if selected_labels else "").strip()
    file_name = f"Vector_Bench_{_safe_download_filename(label)}.xlsx"
    container.download_button(
        label="Download Excel",
        data=excel_bytes,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"download_excel_btn_{pageName}",
    )


def saveAsImage(container, pageName):
    html2canvasJS = load_unpkg(HTML_2_CANVAS_URL)
    container.write()
    buttonText = "Save as Image"
    saveImageButton = container.button(buttonText)
    if saveImageButton:
        components.html(
            _make_capture_script(html2canvasJS, buttonText, pageName, as_pdf=False),
            height=0,
            width=0,
        )


def saveAsPDF(container, pageName):
    html2canvasJS = load_unpkg(HTML_2_CANVAS_URL)
    jspdfJS = load_unpkg(JS_PDF_URL)
    buttonText = "Save as PDF"
    savePDFButton = container.button(buttonText)
    if savePDFButton:
        components.html(
            _make_capture_script(html2canvasJS, buttonText, pageName, as_pdf=True, jspdf_js=jspdfJS),
            height=0,
            width=0,
        )


def _make_capture_script(html2canvas_js: str, button_text: str, page_name: str, as_pdf: bool, jspdf_js: str = ""):
    """Build the inline script that captures the main content and downloads as image or PDF."""
    opts = "allowTaint: false, useCORS: true, scrollY: -window.scrollY, scrollX: -window.scrollX"
    if as_pdf:
        return f"""
<script>{html2canvas_js}</script>
<script>{jspdf_js}</script>
<script>
(function() {{
  const html2canvas = window.html2canvas;
  const jsPDF = window.jspdf && window.jspdf.jsPDF;

  const streamlitDoc = window.parent.document;
  const stApp = streamlitDoc.querySelector('.main .block-container');
  if (!stApp) return;

  const buttons = Array.from(streamlitDoc.querySelectorAll('.stButton > button'));
  const btn = buttons.find(el => el.innerText === '{button_text}');
  if (btn) btn.innerText = 'Creating PDF...';

  const opts = {{ {opts} }};
  html2canvas(stApp, opts).then(function(canvas) {{
    const imgData = canvas.toDataURL('image/png');
    const pdf = new jsPDF('p', 'mm', 'a4');
    const pdfW = 210;
    const pdfH = 297;
    const imgW = pdfW;
    const imgH = (canvas.height * pdfW) / canvas.width;
    let heightLeft = imgH;
    let position = 0;

    pdf.addImage(imgData, 'PNG', 0, position, imgW, imgH);
    heightLeft -= pdfH;

    while (heightLeft > 0) {{
      position = heightLeft - imgH;
      pdf.addPage();
      pdf.addImage(imgData, 'PNG', 0, position, imgW, imgH);
      heightLeft -= pdfH;
    }}

    const dataUrl = pdf.output('datauristring');
    const parentDoc = window.parent.document;
    const a = parentDoc.createElement('a');
    a.href = dataUrl;
    a.download = '{page_name}.pdf';
    a.style.display = 'none';
    parentDoc.body.appendChild(a);
    a.click();
    setTimeout(function() {{ a.remove(); }}, 300);
    if (btn) btn.innerText = '{button_text}';
  }});
}})();
</script>"""
    else:
        return f"""
<script>{html2canvas_js}</script>
<script>
(function() {{
  const html2canvas = window.html2canvas;
  const streamlitDoc = window.parent.document;
  const stApp = streamlitDoc.querySelector('.main .block-container');
  if (!stApp) return;

  const buttons = Array.from(streamlitDoc.querySelectorAll('.stButton > button'));
  const btn = buttons.find(el => el.innerText === '{button_text}');
  if (btn) btn.innerText = 'Creating Image...';

  html2canvas(stApp, {{ {opts} }}).then(function(canvas) {{
    const a = document.createElement('a');
    a.href = canvas.toDataURL("image/png").replace("image/png", "image/octet-stream");
    a.download = '{page_name}.png';
    a.click();
    if (btn) btn.innerText = '{button_text}';
  }});
}})();
</script>"""
