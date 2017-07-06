import sys
import time

from meta.source import Source


class Destination(object):
    _log = None

    ##
    # Initialize the destination
    #
    # @param chunk_size     How large shall the chunks be that will be fetched from the source
    # @param uri            The uri of the source
    # @param force          Force overwrite existing data
    def __init__(self, uri: str, force: bool = False):
        self.uri = uri
        self.force = force

        self.check_destination()

    ##
    # Checks if the destination can be loaded. If something is wrong, thrown an exception
    def check_destination(self):
        raise NotImplementedError("Implement me")

    ##
    # Write the settings for the given index to the datasource
    #
    # @param idx    The index to return the settings for
    def write_settings(self, idx: str, settings: dict, args) -> bool:
        raise NotImplementedError("Implement me")

    ##
    # Write the mappings for the given index to the datasource
    #
    # @param idx    The index to return the mappings for
    def write_mappings(self, idx: str, mappings: dict) -> bool:
        raise NotImplementedError("Implement me")

    ##
    # Write all aliases for the given index to the datasource
    #
    # @param idx    The alias to return the aliases for
    def write_aliases(self, idx: str, aliases: dict) -> bool:
        raise NotImplementedError("Implement me")

    ##
    # This method creates bulk inserts for the given server and index, to speed up the insert into a server
    #
    # @param srv    The destination server which should receive the data
    # @param idx    The index to insert the data at
    # @param type   The type of the data
    # @param data   The actual data to insert

    def bulk_insert_data(self, idx: str, data: list):
        raise NotImplementedError("Implement me")

    ##
    # This method is called at the very end and can be used to to cleanup or final work
    def finish(self):
        raise NotImplementedError("Implement me")

    ##
    # Indicates that work on the current index is done
    def finish_index(self):
        raise NotImplementedError("Implement me")

    ##
    # Retrieves the data for the given index and source to the destination datasource
    #
    # @param src        The source where the data should be retrieved
    # @param idx        The name of the index to return the data for
    def copy_data(self, src: Source, idx: str) -> bool:
        self._log.info("Start copy %s from %s to %s", idx, src.uri, self.uri)

        # copy typewise
        start = time.perf_counter()

        data = src.get_next(idx=idx)
        processed = 0

        self._log.info("Total hits: %i", src.total_hits)

        try:
            while len(data) > 0:
                # exec.submit(bulk_insert_data, dest, idx, type, data["hits"]["hits"])
                self.bulk_insert_data(idx, data)

                processed += len(data)
                qps = (processed / (time.perf_counter() - start))
                seconds = ((src.total_hits - processed) / qps)
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)

                self._log.info("%i of %i with %0.1fqps ~%d:%02d:%02d remaining for %s", processed,
                               src.total_hits, qps, h, m, s, idx)

                data = src.get_next(idx=idx)
        except Exception as e:
            self._log.debug(e, e.__traceback__)

            # log.debug(data)
            sys.exit(0)

        self.finish_index()

        return True
