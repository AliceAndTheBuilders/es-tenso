import logging

import requests
import http.client

# Set a larger http header size
http.client._MAXHEADERS = 1000

class Elastic:
    # The version of the elasticsearch
    __version = None
    __default_header = {'content-type': 'application/json'}
    _log = logging.getLogger("tenso_logger")

    auth = None

    ##
    #
    #
    # @param uri            The uri of the elasticsearch server
    # @param chunk_size     How large shall the chunks be that will be fetched from the source
    # @param auth_user      The HTTP basic auth user
    # @param auth_pass      The HTTP basic auth password
    # @param scroll_time    The time out for the elasticsearch scroll

    def __init__(self, uri: str, auth_user=None, auth_pass=None):
        self.uri = uri

        # Set http basic auth data
        if auth_user is not None and auth_pass is not None:
            self.auth = (auth_user, auth_pass)

    ####################################################################################################################
    # Query - Helper
    ####################################################################################################################

    def get(self, path: str = ""):
        return requests.get(self.uri + path, auth=self.auth)

    def head(self, path: str = ""):
        return requests.head(self.uri + path)

    def put(self, path: str, body: str = None):
        data = None

        if body is not None:
            data = bytes(body, "utf-8")

        return requests.put(self.uri + path, data, auth=self.auth, headers=self.__default_header)

    def post(self, path: str, body: str = None):
        data = None

        if body is not None:
            data = bytes(body, "utf-8")

        return requests.post(self.uri + path, data, auth=self.auth, headers=self.__default_header)

    def delete(self, path: str, body: str = None):
        data = None

        if body is not None:
            data = bytes(body, "utf-8")

        return requests.delete(self.uri + path, data=data, auth=self.auth, headers=self.__default_header)

    ####################################################################################################################
    # Helper
    ####################################################################################################################

    def exists(self, idx: str) -> bool:
        r = self.head(idx)

        return r.status_code == 200

    def wait_for(self, status: str) -> bool:
        self._log.info("Waiting for %s to become %s", self.uri, status)

        r = self.get("_cluster/health?timeout=60s&wait_for_status=" + status)

        return r.status_code == 200

    def close_index(self, idx: str) -> bool:
        self._log.info("Closing %s on %s", idx, self.uri)

        self.wait_for("yellow")

        r = self.post(idx + "/_close")

        if r.status_code == 200:
            return True

        self._log.error("Status %s -> %s", r.status_code, r.json())

        return False

    def open_index(self, idx: str) -> bool:
        self._log.info("Opening %s on %s", idx, self.uri)

        self.wait_for("yellow")

        r = self.post(idx + "/_open")

        if r.status_code == 200:
            return True

        self._log.error("Status %s -> %s", r.status_code, r.json())

        return False
