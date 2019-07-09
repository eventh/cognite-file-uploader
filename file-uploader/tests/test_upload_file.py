# coding: utf-8
"""
A module for testing the extractor.
"""
from pathlib import Path

from upload_file import FileWithMeta, convert_to_file_objects, match_files


class TestExtractor:
    folder_path = Path(__file__).parent / "test-files"

    def test_match_files(self):
        assert len(match_files(self.folder_path)) == 2
        files = match_files(self.folder_path, "*.pdf")
        assert len(files) == 1
        assert files[0].name == "hidden.pdf"
        assert len(match_files(self.folder_path, "*", recursive=False)) == 1

    def test_convert_to_file_objects(self):
        paths = [self.folder_path / "example.txt"]
        objects = convert_to_file_objects(self.folder_path, paths)
        assert len(objects) == 1
        assert objects[0].name == "example.txt"

    def test_convert_metadata(self):
        path = self.folder_path / "recursive" / "hidden.pdf"
        obj = FileWithMeta.from_path(path, self.folder_path)
        assert obj.metadata
        assert obj.metadata["folder"] == "recursive"
        assert obj.name == "hidden.pdf"
        assert obj.path == path
