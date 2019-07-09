#!/usr/bin/env python
# coding: utf-8
"""
A script that process files in a specified folder,
to upload metadata and files to CDF.

Use the command line arguments for controlling uploading of
metadata to raw, and files to CDF.
"""
import argparse
import logging
import mimetypes
import os
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Sequence

import google.cloud.logging
from cognite.client import CogniteClient
from cognite.client.data_classes.raw import Row
from cognite.client.exceptions import CogniteAPIError

logger = logging.getLogger(__name__)
COGNITE_CLIENT_NAME = "cognite-file-extractor-python"


class FileWithMeta:
    """A container object for all data we extract from filesystem for a single file."""

    def __init__(self, path, external_id, name, mime_type=None, metadata=None):
        self.path = path
        self.external_id = external_id
        self.name = name
        self.mime_type = mime_type
        self.metadata = metadata

    @classmethod
    def from_path(cls, path: Path, root_path: Path):
        external_id = str(path.relative_to(root_path))
        mime_type = mimetypes.guess_type(path.name)[0]

        folder_path = str(path.relative_to(root_path).parent)
        if folder_path:
            metadata = {"folder": folder_path}
            metadata.update({"col%s" % i: o for i, o in enumerate(folder_path.split(os.path.sep))})
        else:
            metadata = {}

        return cls(path.resolve(), external_id, path.name, mime_type, metadata=metadata)

    def raw_columns(self):
        obj = {"name": self.name, "external_id": self.external_id}
        if self.mime_type:
            obj["mime_type"] = self.mime_type
        obj.update(self.metadata)
        return obj


def _parse_cli_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", "-i", type=Path, required=True, help="Folder path of the files to process")
    parser.add_argument("--pattern", "-p", required=False, default="*", help="Filename pattern to match against")
    parser.add_argument("--api-key", "-k", required=False, help="CDF API KEY")
    parser.add_argument(
        "--log", type=Path, required=False, default=Path(__file__).absolute().parent / "log", help="Log folder"
    )
    parser.add_argument("--log-level", required=False, default="INFO", help="Logging level")
    parser.add_argument("--upload-to-cdf", required=False, action="store_true", help="Upload files to CDF")
    parser.add_argument("--no-overwrite", required=False, action="store_true", help="Do not overwrite uploaded file")
    parser.add_argument(
        "--ignore-meta", required=False, action="store_true", help="Ignore metadata when uploading file"
    )
    parser.add_argument("--upload-to-raw", required=False, action="store_true", help="Upload metadata to raw")
    parser.add_argument("--raw-db", required=False, default="LandingZone", help="Which raw database")
    parser.add_argument("--raw-table", required=False, default="FileExtractor", help="Which table in raw")
    return parser.parse_args()


def _configure_logger(folder_path: Path, log_level: str) -> None:
    """Create 'folder_path' and configure logging to file as well as console."""
    folder_path.mkdir(parents=True, exist_ok=True)
    log_file = folder_path.joinpath("file-uploader-python.log")
    logging.basicConfig(
        level=logging.INFO if log_level == "INFO" else log_level,
        format="%(asctime)s %(name)s %(levelname)s - %(message)s",
        handlers=[
            TimedRotatingFileHandler(log_file, when="midnight", backupCount=7),
            logging.StreamHandler(sys.stdout),
        ],
    )

    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):  # Temp hack
        google.cloud.logging.Client().setup_logging(name="file-uploader-python")


def read_all_files(root_path: Path, filename_pattern: str = "*", recursive: bool = True) -> Sequence[Path]:
    paths = root_path.rglob(filename_pattern) if recursive else root_path.glob(filename_pattern)
    filtered_paths = [path for path in paths if path.is_file() and not path.name.startswith(".")]
    logger.info("Found {} files in {!s}".format(len(filtered_paths), root_path))
    return filtered_paths


def upload_metadata_to_raw(client: CogniteClient, objects: Sequence[FileWithMeta], database: str, table: str):
    rows = [Row(obj.external_id, obj.raw_columns()) for obj in objects]
    start_time = time.time()
    client.raw.rows.insert(database, table, rows, ensure_parent=True)
    logger.info(
        "Uploaded {} rows to raw:{}:{} in {:.2f} seconds".format(len(rows), database, table, time.time() - start_time)
    )


def upload_files_to_cdf(
    client: CogniteClient, objects: Sequence[FileWithMeta], overwrite: bool = True, ignore_meta: bool = False
) -> None:
    for i, obj in enumerate(objects):
        file_index = "[{}:{}]".format(i, len(objects) - 1)
        logger.debug("{} Starting upload of {}".format(file_index, obj.path))
        start_time = time.time()
        try:
            res = client.files.upload(
                obj.path,
                name=obj.name,
                external_id=obj.external_id,
                mime_type=obj.mime_type,
                metadata=obj.metadata if not ignore_meta else None,
                overwrite=overwrite,
            )
        except CogniteAPIError as exc:
            logger.error("Failed to upload {}: {!s}".format(obj.external_id, exc))
        else:
            logger.info(
                "{} Finished upload of {} in {:.2f} seconds".format(
                    file_index, obj.external_id, time.time() - start_time
                )
            )
            logger.debug("{} {!s}".format(file_index, res))


def main(args):
    _configure_logger(args.log, args.log_level)

    api_key = args.api_key if args.api_key else os.environ.get("COGNITE_EXTRACTOR_API_KEY")
    args.api_key = ""  # Don't log the api key if given through CLI
    logger.info("Extractor configured with {}".format(args))

    if not args.input_dir.exists():
        logger.fatal("Input folder does not exists: {!s}".format(args.input_dir))
        sys.exit(2)
    root_path = args.input_dir

    try:
        client = CogniteClient(api_key=api_key, client_name=COGNITE_CLIENT_NAME)
        logger.info(client.login.status())
    except CogniteAPIError as exc:
        logger.error("Failed to create CDF client: {!s}".format(exc))
        client = CogniteClient(api_key=api_key, client_name=COGNITE_CLIENT_NAME)

    try:
        file_paths = read_all_files(root_path, args.pattern)
        file_objects = [FileWithMeta.from_path(p, root_path) for p in file_paths]

        if args.upload_to_raw:
            upload_metadata_to_raw(client, file_objects, args.raw_db, args.raw_table)
        if args.upload_to_cdf:
            upload_files_to_cdf(client, file_objects, not args.no_overwrite, args.ignore_meta)

    except KeyboardInterrupt:
        logger.warning("Extractor stopped")
    except Exception as exc:
        logger.error(str(exc), exc_info=exc)


if __name__ == "__main__":
    main(_parse_cli_args())
