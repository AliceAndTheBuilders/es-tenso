class Source(object):
    _total_hits = None

    @property
    def total_hits(self) -> int:
        return self._total_hits

    ##
    # Initialize the source
    #
    # @param chunk_size     How large shall the chunks be that will be fetched from the source
    # @param uri            The uri of the source
    def __init__(self, uri: str, chunk_size: int = 1000):
        self.chunk_size = chunk_size
        self.uri = uri

        self.check_source()

    ##
    # Checks if the source can be loaded. If something is wrong, thrown an exception
    def check_source(self):
        raise NotImplementedError("Implement me")

    ##
    # Return a list of all available indices
    def get_indices(self) -> list:
        raise NotImplementedError("Implement me")

    ##
    # Get the settings for the given index on the datasource
    #
    # @param idx    The index to return the settings for
    def get_settings(self, idx: str) -> dict:
        raise NotImplementedError("Implement me")

    ##
    # Get the mappings for the given index on the datasource
    #
    # @param idx    The index to return the mappings for
    def get_mappings(self, idx: str) -> dict:
        raise NotImplementedError("Implement me")

    ##
    # Returns all aliases for the given index
    #
    # @param idx    The alias to return the aliases for
    def get_aliases(self, idx: str) -> dict:
        raise NotImplementedError("Implement me")

    ##
    # This method should return the next chunk of data, but at most <chunk_size> number of items. If there are no data
    # left to return, an empty list will be returned.
    #
    # @param idx        The name of the index to return the data for
    # @param chunk_size The size of the data chunk to return
    def get_next(self, idx: str, chunk_size: int = 1000) -> list:
        raise NotImplementedError("Implement me")
