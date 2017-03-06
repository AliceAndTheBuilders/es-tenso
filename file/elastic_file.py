import json
import logging
import os
import tempfile
import zipfile


class ElasticFile:
    _log = logging.getLogger("tenso_logger")

    tmp_dir = None

    def __init__(self, uri: str):
        self.uri = uri

    ####################################################################################################################
    # Helper
    ####################################################################################################################

    def get_from_zip(self, idx: str, file_name: str) -> json:
        z = zipfile.ZipFile(self.uri)
        self._log.debug(z.getinfo(idx + "/" + file_name))

        f = z.open(idx + "/" + file_name)

        return json.loads(f.read().decode("utf-8"))

    def write_to_file(self, idx: str, file_name: str, data: object):
        file = self.get_file(idx, file_name)
        file.write(json.dumps(data))
        self._log.debug("%s size is now %s", file.name, ElasticFile.sizeof_fmt(file.tell()))
        file.close()

    def get_file(self, idx: str, file_name: str):
        if self.tmp_dir is None:
            self.tmp_dir = tempfile.mkdtemp()
            self._log.debug("Created tmp directory: %s", self.tmp_dir)

        idx_dir = self.tmp_dir + "/" + idx

        if not os.path.isdir(idx_dir):
            self._log.debug("Creating directory: %s", idx_dir)
            os.mkdir(idx_dir)

        type_file = idx_dir + "/" + file_name

        if not os.path.isfile(type_file):
            self._log.debug("Creating file: %s", type_file)

        return open(type_file, 'a')

    ##
    # Returns the size in a human readable format
    # Thanks to http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    @staticmethod
    def sizeof_fmt(num: int, suffix='B') -> str:
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0

        return "%.1f%s%s" % (num, 'Yi', suffix)
