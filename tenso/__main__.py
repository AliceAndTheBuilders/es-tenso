import argparse
import logging
from time import strftime, gmtime

from meta.helper import Helper
from version import __version__


def main():
    ####################################################################################################################
    # Arguments
    ####################################################################################################################

    parser = argparse.ArgumentParser()

    # Important
    parser.add_argument("source", help="The source of the data, e.g. http://1.2.3.4:9200 or dump.zip")
    parser.add_argument("destination",
                        help="The destination for the data, e.g. http://4.3.2.1:9200 or mydump.zip. If this "
                             "is not given a zip dump will be created automatically.",
                        default="dump_" + strftime("%Y-%m-%d_%H-%M-%S", gmtime()) + ".zip", nargs='?')

    # Source arguments
    parser.add_argument("--source_auth_user", help="The user for HTTP Basic Auth", default=None)
    parser.add_argument("--source_auth_pass", help="The password for HTTP Basic Auth", default=None)
    # TODO implement for file sources
    parser.add_argument("-cs", "--chunk_size",
                        help="How many docs shall the chunks that will be added at once have at max. This currently "
                             "only works with elasticsearch as datasource.",
                        default=1000)
    parser.add_argument("-st", "--scroll_time", help="The value to use for elasticsearch scroll time.", default="1m")

    # Destination arguments
    parser.add_argument("--dest_auth_user", help="The user for HTTP Basic Auth", default=None)
    parser.add_argument("--dest_auth_pass", help="The password for HTTP Basic Auth", default=None)
    parser.add_argument("--max_file_size", help="Maximum file size for file destination", default=100)
    # TODO implement as feature
    # parser.add_argument("-f", "--force", help="Force overwrite existing destinations", action="store_true")

    # Behaviour modifiers
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("-v", "--verbose", help="More verbose output", action="store_true")
    output_group.add_argument("-q", "--quiet", help="Be quiet", action="store_true")
    # TODO implement
    # parser.add_argument("-i", "--indices", help="Specify a list of indices that will be used, e.g. a, b, c, d.")

    # parser.add_argument("-nz", "--no-zip", help="If this is given the destination will be treated as a directory and
    # no zipping will be done.", action="store_true")

    args = parser.parse_args()

    ####################################################################################################################
    # Configure logging
    ####################################################################################################################

    ch = logging.StreamHandler()

    if args.verbose:
        ch.setLevel(logging.DEBUG)
    elif args.quiet:
        ch.setLevel(logging.WARN)
    else:
        ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    log = logging.getLogger("tenso_logger")
    log.addHandler(ch)
    log.setLevel(level=logging.DEBUG)

    ####################################################################################################################
    # Startup
    ####################################################################################################################

    log.info("Elasticsearch Tensō - 転送")
    log.info("Version: %s", __version__)

    src = Helper.get_source(args=args)
    dest = Helper.get_destination(args=args)

    log.info("Source: %s", src.uri)
    log.info("Destination: %s", dest.uri)

    ####################################################################################################################
    # Process data
    ####################################################################################################################

    indices = src.get_indices()

    log.info("%i Indices: %s", len(indices), indices)

    for idx in indices:
        log.info("#################################### %s ####################################", idx)

        log.info("### SETTINGS ###")
        settings = src.get_settings(idx=idx)
        dest.write_settings(idx=idx, settings=settings)

        log.info("### MAPPINGS ###")
        mappings = src.get_mappings(idx=idx)
        dest.write_mappings(idx=idx, mappings=mappings)

        log.info("### ALIASES ###")
        aliases = src.get_aliases(idx=idx)
        dest.write_aliases(idx=idx, aliases=aliases)

        log.info("### DATA ###")
        dest.copy_data(src=src, idx=idx)

    # Finish all
    dest.finish()

    log.info("Sayōnara - さようなら")


if __name__ == "__main__":
    main()
