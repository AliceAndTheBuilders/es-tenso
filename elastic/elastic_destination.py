import json
from distutils.version import LooseVersion

from elastic.elastic import Elastic
from meta.destination import Destination


class ElasticDestination(Elastic, Destination):
    def __init__(self, uri: str, auth_user=None, auth_pass=None, force: bool = False):
        uri += "/"

        self.force = force

        Elastic.__init__(self, uri=uri, auth_user=auth_user, auth_pass=auth_pass)
        Destination.__init__(self, uri=uri)

    ####################################################################################################################
    # Destination
    ####################################################################################################################

    def check_destination(self):
        r = self.get()

        if r.status_code != 200:
            raise Exception("Could not connect to %s: %s -> %s", self.uri, r.status_code, r.text)

        self.__version = LooseVersion(r.json()["version"]["number"])

        self._log.info("Destination Elasticsearch: %s", self.__version.vstring)

    def write_mappings(self, idx: str, mappings: dict) -> bool:
        # Split the dict by types and create single mapping requests
        for document_type in mappings[idx]["mappings"]:
            self._log.info("Writing mappings for %s in index %s", document_type, idx)

            body = json.dumps(mappings[idx]["mappings"][document_type])

            r = self.put(idx + "/_mapping/" + document_type, body)

            if r.status_code != 200:
                self._log.debug(body)
                raise Exception("Could not write mappings", r.text)

        return True

    def write_settings(self, idx: str, settings: dict) -> bool:
        # Sanitize index request
        allowed = ["number_of_replicas", "number_of_shards"]

        sanitized_list = {"settings": {"index": {}}}

        for item in settings[idx]["settings"]["index"].items():
            if item[0] in allowed:
                sanitized_list["settings"]["index"][item[0]] = item[1]

        if self.exists(idx):
            self._log.warning("%s does already exist", idx)
            return False

        body = json.dumps(sanitized_list)

        r = self.put(idx, body)

        self._log.info("Adding %s with %s", idx, body)

        if r.status_code != 200:
            self._log.error("Could not create index %s: %s -> %s", idx, r.status_code, r.json()["error"]["reason"])

        if "analysis" in settings[idx]["settings"]["index"]:
            self.close_index(idx)

            for analysis_type in settings[idx]["settings"]["index"]["analysis"]:
                for thingy in settings[idx]["settings"]["index"]["analysis"][analysis_type]:
                    body = {
                        "analysis": {
                            analysis_type: {
                                thingy: settings[idx]["settings"]["index"]["analysis"][analysis_type][thingy]
                            }
                        }
                    }

                    r = self.put(idx + "/_settings", json.dumps(body))

                    self._log.info("Adding %s with %s: %s", idx, body, r.text)

            self.open_index(idx)

        return True

    def write_aliases(self, idx: str, aliases: dict) -> bool:
        for alias in aliases:
            self._log.debug("Writing alias %s", alias)

            self.put(idx + "/_alias/" + alias, json.dumps(aliases[alias]))

        return True

    ##
    # This method creates bulk inserts for the given server and index, to speed up the insert into a server
    #
    # @param srv    The destination server which should receive the data
    # @param idx    The index to insert the data at
    # @param type   The type of the data
    # @param data   The actual data to insert

    def bulk_insert_data(self, idx: str, data: list):
        bulk = []

        # Create bulk data json
        # TODO Maybe this could be enhanced to automagically slice the bulk request to not break es chunk size
        for row in data:
            bulk.append(json.dumps({"create": {"_index": idx, "_type": row["_type"], "_id": row["_id"]}}) + "\n")
            bulk.append(json.dumps(row["_source"]) + "\n")

        # Fire the bulk request
        self.post("_bulk", ''.join(bulk))

    def finish_index(self):
        pass

    def finish(self):
        pass
