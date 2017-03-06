import os
import zipfile

from file.elastic_file import ElasticFile
from meta.source import Source


class FileSource(ElasticFile, Source):
    cur_idx = None
    cur_ptr = 0

    def __init__(self, uri: str, chunk_size: int):
        ElasticFile.__init__(self, uri=uri)
        Source.__init__(self, uri=uri, chunk_size=chunk_size)

    ####################################################################################################################
    # Source
    ####################################################################################################################

    def check_source(self):
        if os.path.isdir(self.uri):
            raise Exception(self.uri + " is an existing directory, please specify a file.")

        if not os.path.isfile(self.uri):
            raise Exception(self.uri + " does not exist")

    def get_mappings(self, idx: str) -> dict:
        mappings = self.get_from_zip(idx, "mappings.json")

        return mappings

    def get_settings(self, idx: str) -> dict:
        settings = self.get_from_zip(idx, "settings.json")

        return settings

    def get_aliases(self, idx: str) -> dict:
        return self.get_from_zip(idx, "aliases.json")

    def get_indices(self) -> list:
        dirs = []

        for item in zipfile.ZipFile(self.uri).namelist():
            path = item.split("/", 1)[0]
            if path not in dirs:
                dirs.append(path)

        return dirs

    def get_next(self, idx: str, chunk_size: int = 1000) -> list:
        if self.cur_idx is None or self.cur_idx != idx:
            self._total_hits = self.get_from_zip(idx, "meta.json")["total_hits"]
            self.cur_idx = idx
            self.cur_ptr = 0

        try:
            data = self.get_from_zip(idx, "data_" + str(self.cur_ptr) + ".json")
        except KeyError:
            return []

        self.cur_ptr += 1

        return data
