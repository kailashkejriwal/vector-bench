import logging
import pathlib
import typing
from abc import ABC, abstractmethod
from enum import Enum

from tqdm import tqdm

from vectordb_bench import config

logging.getLogger("s3fs").setLevel(logging.CRITICAL)

log = logging.getLogger(__name__)

DatasetReader = typing.TypeVar("DatasetReader")


def _parquet_readable(path: pathlib.Path) -> bool:
    """Return False if the parquet file is corrupted or unreadable (e.g. incomplete download, ZSTD error).

    We read the first batch of data (not just schema) so that ZSTD decompression runs and
    corruption in the column data is detected.
    """
    if path.suffix.lower() != ".parquet":
        return True
    try:
        import pyarrow.parquet as pq

        with open(path, "rb") as f:
            reader = pq.ParquetFile(f)
            # Read first row group to trigger ZSTD decompression; catches data corruption
            if reader.num_row_groups:
                reader.read_row_group(0)
            else:
                reader.read_schema()
        return True
    except Exception as e:
        log.info("Parquet file %s not readable (will re-download): %s", path, e)
        return False


class DatasetSource(Enum):
    S3 = "S3"
    AliyunOSS = "AliyunOSS"

    def reader(self) -> DatasetReader:
        if self == DatasetSource.S3:
            return AwsS3Reader()

        if self == DatasetSource.AliyunOSS:
            return AliyunOSSReader()

        return None


class DatasetReader(ABC):
    source: DatasetSource
    remote_root: str

    @abstractmethod
    def read(self, dataset: str, files: list[str], local_ds_root: pathlib.Path):
        """read dataset files from remote_root to local_ds_root,

        Args:
            dataset(str): for instance "sift_small_500k"
            files(list[str]):  all filenames of the dataset
            local_ds_root(pathlib.Path): whether to write the remote data.
        """

    @abstractmethod
    def validate_file(self, remote: pathlib.Path, local: pathlib.Path) -> bool:
        pass


class AliyunOSSReader(DatasetReader):
    source: DatasetSource = DatasetSource.AliyunOSS
    remote_root: str = config.ALIYUN_OSS_URL

    def __init__(self):
        import oss2

        self.bucket = oss2.Bucket(oss2.AnonymousAuth(), self.remote_root, "benchmark", True)

    def validate_file(self, remote: pathlib.Path, local: pathlib.Path) -> bool:
        info = self.bucket.get_object_meta(remote.as_posix())

        # check size equal
        remote_size, local_size = info.content_length, local.stat().st_size
        if remote_size != local_size:
            log.info(f"local file: {local} size[{local_size}] not match with remote size[{remote_size}]")
            return False

        if not _parquet_readable(local):
            return False

        return True

    def read(self, dataset: str, files: list[str], local_ds_root: pathlib.Path):
        downloads = []
        if not local_ds_root.exists():
            log.info(f"local dataset root path not exist, creating it: {local_ds_root}")
            local_ds_root.mkdir(parents=True)
            downloads = [
                (
                    pathlib.PurePosixPath("benchmark", dataset, f),
                    local_ds_root.joinpath(f),
                )
                for f in files
            ]

        else:
            # Use local files when all exist, have size > 0, and parquets are readable (no remote call)
            def ok(f: str) -> bool:
                p = local_ds_root.joinpath(f)
                if not p.exists() or p.stat().st_size <= 0:
                    return False
                return _parquet_readable(p)

            all_present = all(ok(f) for f in files)
            if all_present:
                log.info(f"All {len(files)} files present locally at {local_ds_root}, skipping remote fetch")
                return

            for file in files:
                remote_file = pathlib.PurePosixPath("benchmark", dataset, file)
                local_file = local_ds_root.joinpath(file)

                if (not local_file.exists()) or (not self.validate_file(remote_file, local_file)):
                    log.info(f"local file: {local_file} not match with remote: {remote_file}; add to downloading list")
                    downloads.append((remote_file, local_file))

        if len(downloads) == 0:
            return

        log.info(f"Start to downloading files, total count: {len(downloads)}")
        for remote_file, local_file in tqdm(downloads):
            log.debug(f"downloading file {remote_file} to {local_file}")
            for attempt in range(2):  # initial + one retry on corruption
                self.bucket.get_object_to_file(remote_file.as_posix(), local_file.absolute())
                if _parquet_readable(local_file):
                    break
                log.warning("Downloaded file %s failed integrity check (attempt %s), re-downloading", local_file.name, attempt + 1)
                local_file.unlink(missing_ok=True)
            else:
                log.warning("File %s still corrupted after retry; continuing", local_file.name)
        log.info(f"Succeed to download all files, downloaded file count = {len(downloads)}")


class AwsS3Reader(DatasetReader):
    source: DatasetSource = DatasetSource.S3
    remote_root: str = config.AWS_S3_URL

    def __init__(self):
        import s3fs

        # Timeouts so downloads don't hang indefinitely (connect 15s, read 10min per file)
        self.fs = s3fs.S3FileSystem(
            anon=True,
            client_kwargs={"region_name": "us-west-2"},
            config_kwargs={"connect_timeout": 15, "read_timeout": 600},
        )

    def ls_all(self, dataset: str):
        dataset_root_dir = pathlib.Path(self.remote_root, dataset)
        log.info(f"listing dataset: {dataset_root_dir}")
        names = self.fs.ls(dataset_root_dir)
        for n in names:
            log.info(n)
        return names

    def read(self, dataset: str, files: list[str], local_ds_root: pathlib.Path):
        downloads = []
        if not local_ds_root.exists():
            log.info(f"local dataset root path not exist, creating it: {local_ds_root}")
            local_ds_root.mkdir(parents=True)
            downloads = [pathlib.PurePosixPath(self.remote_root, dataset, f) for f in files]

        else:
            # Use local files when all exist, have size > 0, and parquets are readable (no remote call)
            def ok(f: str) -> bool:
                p = local_ds_root.joinpath(f)
                if not p.exists() or p.stat().st_size <= 0:
                    return False
                return _parquet_readable(p)

            all_present = all(ok(f) for f in files)
            if all_present:
                log.info(f"All {len(files)} files present locally at {local_ds_root}, skipping remote fetch")
                return

            for file in files:
                remote_file = pathlib.PurePosixPath(self.remote_root, dataset, file)
                local_file = local_ds_root.joinpath(file)

                if (not local_file.exists()) or (not self.validate_file(remote_file, local_file)):
                    log.info(f"local file: {local_file} not match with remote: {remote_file}; add to downloading list")
                    downloads.append(remote_file)

        if len(downloads) == 0:
            return

        log.info(f"Start to downloading files, total count: {len(downloads)}, {downloads}")
        log.info(
            "If the progress bar does not move, your network may block S3. "
            "On a corporate network, set HTTP_PROXY/HTTPS_PROXY/NO_PROXY if required, or use VPN."
        )
        for s3_file in tqdm(downloads, desc="Files", unit="file"):
            local_path = local_ds_root / pathlib.Path(s3_file).name
            log.info("Downloading %s -> %s", s3_file, local_path)
            try:
                info = self.fs.info(s3_file)
                size = info.get("size") or 0
            except Exception as e:
                log.warning("Could not get size for %s: %s; downloading without progress", s3_file, e)
                size = None
            for attempt in range(2):  # initial + one retry on corruption
                if size and size > 0:
                    with tqdm(total=size, desc=pathlib.Path(s3_file).name, unit="B", unit_scale=True, unit_divisor=1024) as pbar:
                        with self.fs.open(s3_file, "rb") as r:
                            with open(local_path, "wb") as w:
                                while True:
                                    chunk = r.read(1024 * 1024)
                                    if not chunk:
                                        break
                                    w.write(chunk)
                                    pbar.update(len(chunk))
                else:
                    self.fs.download(s3_file, local_ds_root.as_posix())
                if _parquet_readable(local_path):
                    break
                log.warning("Downloaded file %s failed integrity check (attempt %s), re-downloading", local_path.name, attempt + 1)
                local_path.unlink(missing_ok=True)
            else:
                log.warning("File %s still corrupted after retry; continuing", local_path.name)

        log.info(f"Succeed to download all files, downloaded file count = {len(downloads)}")

    def validate_file(self, remote: pathlib.Path, local: pathlib.Path) -> bool:
        # info() uses ls() inside, maybe we only need to ls once
        info = self.fs.info(remote)

        # check size equal
        remote_size, local_size = info.get("size"), local.stat().st_size
        if remote_size != local_size:
            log.info(f"local file: {local} size[{local_size}] not match with remote size[{remote_size}]")
            return False

        if not _parquet_readable(local):
            return False

        return True
