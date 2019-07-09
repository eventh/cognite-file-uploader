# coding: utf-8
"""
A module for testing the extractor.
"""
from pathlib import Path

from upload_file import read_all_files


class TestExtractor:
    folder_path = Path(__file__).parent / "test-files"

    def test_find_files_in_path_historical(self):
        files = read_all_files(self.folder_path)
        assert len(files) == 1
