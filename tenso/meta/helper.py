import requests
from requests.exceptions import MissingSchema

from elastic.elastic_destination import ElasticDestination
from elastic.elastic_source import ElasticSource
from file.file_destination import FileDestination
from file.file_source import FileSource


class Helper:
    @staticmethod
    def is_uri_reachable(uri: str) -> bool:
        try:
            return requests.head(uri).status_code == requests.codes.ok
        except MissingSchema:
            return False

    ##
    # Identifies the kind of the source and initializes it
    #
    # @param args   The arguments passed to the application
    @staticmethod
    def get_destination(args):
        if Helper.is_uri_reachable(uri=args.destination):
            # Seems to be an URI
            return ElasticDestination(uri=args.destination, force=False, auth_user=args.dest_auth_user,
                                      auth_pass=args.dest_auth_pass, args=args)

        # Default to FileSource
        return FileDestination(uri=args.destination, force=False, max_file_size=args.max_file_size, args=args)

    ##
    # Identifies the kind of the source and initializes it
    #
    # @param args   The arguments passed to the application
    @staticmethod
    def get_source(args):
        if Helper.is_uri_reachable(uri=args.source):
            # Seems to be an URI
            return ElasticSource(uri=args.source, chunk_size=args.chunk_size, auth_user=args.source_auth_user,
                                 auth_pass=args.source_auth_pass, scroll_time=args.scroll_time)

        # Default to FileSource
        return FileSource(uri=args.source, chunk_size=args.chunk_size)
