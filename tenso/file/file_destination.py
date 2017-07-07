import json
import os
import sys
import zipfile

from file.elastic_file import ElasticFile
from meta.destination import Destination


class FileDestination(ElasticFile, Destination):
    cur_data = []
    cur_idx = None
    cur_size = 0
    cur_data_file_no = 0
    docs_written = 0
    max_file_size = None

    def __init__(self, uri: str, force: bool = False, max_file_size: int = 100, args = None):
        self.max_file_size = (int(max_file_size) * 1024 * 1024)

        ElasticFile.__init__(self, uri=uri)
        # Source.__init__(self, uri=uri, chunk_size=chunk_size)
        Destination.__init__(self, uri=uri, force=force, args=args)

    ####################################################################################################################
    # Destination
    ####################################################################################################################

    def check_destination(self):
        if os.path.isdir(self.uri):
            raise Exception(self.uri + " is an existing directory, please specify a file.")

        if os.path.exists(self.uri) and not self.force:
            raise Exception(self.uri + " already exists, use force mode to overwrite")

        if os.path.isfile(self.uri) and self.force:
            self._log.info("Removing file %s", self.uri)
            os.remove(self.uri)

    def index_start(self, idx: str):
        if self.cur_idx is None or self.cur_idx != idx:
            # Change the current index marker
            self.cur_idx = idx
            self.cur_data = []
            self.cur_data_file_no = 0

    def prepare(self, idx: str):
        # Remove file if force is set
        if self.force and os.path.isfile(self.uri):
            os.remove(self.uri)

    def write_settings(self, idx: str, settings: dict) -> bool:
        self.index_start(idx=idx)
        self.write_to_file(idx, "settings.json", settings)

        return True

    def write_mappings(self, idx: str, mappings: dict) -> bool:
        self.index_start(idx=idx)
        self.write_to_file(idx, "mappings.json", mappings)

        return True

    def write_aliases(self, idx: str, aliases: dict) -> bool:
        self.index_start(idx=idx)
        self.write_to_file(idx, "aliases.json", aliases)

        return True

    def bulk_insert_data(self, idx: str, data: list):
        self.index_start(idx=idx)

        self.cur_data.extend(data)
        self.docs_written += len(data)

        # Dirty but workin' ;)
        self.cur_size += sys.getsizeof(json.dumps(data))

        # Write data to file if size exceeds max_data_size
        if self.cur_size > self.max_file_size:
            self.write_to_file(idx, "data_" + str(self.cur_data_file_no) + ".json", self.cur_data)
            self.cur_data = []
            self.cur_size = 0
            self.cur_data_file_no += 1

        return True

    def finish_index(self):
        if len(self.cur_data) > 0:
            # Write out the data
            self.write_to_file(self.cur_idx, "data_" + str(self.cur_data_file_no) + ".json", self.cur_data)

        self.write_to_file(self.cur_idx, "meta.json", {"total_hits": self.docs_written})

        self.cur_data = []
        self.cur_data_file_no = 0
        self.cur_size = 0
        self.docs_written = 0

    def finish(self):
        zf = zipfile.ZipFile(self.uri, 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files in os.walk(self.tmp_dir):
            for file in files:
                cur_file = root + "/" + file
                self._log.debug("Processing and removing %s", cur_file)

                # Use relative file name for zip archive thus remove tmp_dir path
                zf.write(os.path.join(root, file),
                         arcname=os.path.join(root.replace(self.tmp_dir + "/", "", 1), file))
                os.remove(cur_file)

            if root != self.tmp_dir:
                os.rmdir(root)

        zf.close()

        os.rmdir(self.tmp_dir)
