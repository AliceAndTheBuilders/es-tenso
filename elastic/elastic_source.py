import json
from distutils.version import LooseVersion

from elastic.elastic import Elastic
from meta.source import Source


class ElasticSource(Elastic, Source):
    # The id of the current scroll
    __scroll_id = None

    def __init__(self, uri: str, chunk_size: int, auth_user, auth_pass, scroll_time: str):
        uri += "/"

        Elastic.__init__(self, uri=uri, auth_user=auth_user, auth_pass=auth_pass)

        self.scroll_time = scroll_time

        Source.__init__(self, uri=uri, chunk_size=chunk_size)

    ####################################################################################################################
    # Source
    ####################################################################################################################

    def check_source(self):
        r = self.get()

        if r.status_code != 200:
            raise Exception("Could not connect to  %s: %s -> %s", self.uri, r.status_code, r.text)

        self.__version = LooseVersion(r.json()["version"]["number"])

        self._log.info("Source Elasticsearch: %s", self.__version.vstring)

    ##
    # Returns a list of available indices

    def get_indices(self) -> list:
        r = self.get("_settings")

        if r.status_code != 200:
            raise Exception("Could not get indices", r.text)

        return list(r.json().keys())

    ##
    # Return the mappings for the given index
    #
    # @param idx    The index to return the mappings for

    def get_mappings(self, idx: str) -> dict:
        r = self.get(idx + "/_mappings")

        if r.status_code != 200:
            raise Exception("Could not get indices", r.text)

        # return data.decode("utf-8")
        return r.json()

    ##
    # Return all settings for the given index
    #
    # @param idx    The index to return the settings for
    def get_settings(self, idx: str) -> dict:
        r = self.get(idx + "/_settings")

        if r.status_code != 200:
            raise Exception("Could not get index settings", r.text)

        # return data.decode("utf-8")
        return r.json()

    def get_aliases(self, idx: str) -> dict:
        r = self.get(idx + "/_alias")

        if r.status_code != 200:
            raise Exception("Could not get index aliases", r.text)

        data = r.json()

        return data[idx]["aliases"]

    def get_next(self, idx: str, chunk_size: int = None) -> list:
        # Check if we start a new search or continue an existing one
        if self.__scroll_id is None:
            if chunk_size is None:
                chunk_size = self.chunk_size

            body = json.dumps(
                {
                    "size": chunk_size,
                    "sort": ["_doc"]
                }
            )

            r = self.post(idx + "/_search?scroll=" + self.scroll_time, body)

            data = r.json()

            self._total_hits = int(data["hits"]["total"])
        else:
            if self.__version < LooseVersion("5.0"):
                # Since 5.0 we scroll differently
                r = self.get("_search/scroll?scroll=" + self.scroll_time + "&scroll_id=" + self.__scroll_id)

                data = r.json()
            else:
                search_body = {
                    "scroll_id": self.__scroll_id,
                    "scroll": self.scroll_time
                }

                r = self.post("_search/scroll", json.dumps(search_body))

                data = r.json()

        if "_scroll_id" in data:
            self.__scroll_id = data["_scroll_id"]

        if "hits" not in data or "hits" not in data["hits"]:
            if r.status_code != 200:
                raise Exception("Could not find hits" + r.status_code + " -> " + r.text)

            self._log.debug("!!! crazy result: %s", data)

            return []

        # Search done, kill the scroll
        if len(data["hits"]["hits"]) == 0:
            self.delete("_search/scroll", json.dumps({"scroll_id": [self.__scroll_id]}))

            self.__scroll_id = None

        return data["hits"]["hits"]
