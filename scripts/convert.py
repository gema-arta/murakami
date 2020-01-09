"""
Murakami is a tool for creating an automated internet measurement service.
Results are saved as individual files in JSON new line format (.jsonl), and
this is a utility script designed to convert them to other formats.
"""

import difflib
import glob
import logging
import os
import sys

import configargparse
import csv
import jsonlines

logger = logging.getLogger(__name__)

DEFAULT_FORMAT = "csv"
DEFAULT_TEST = "speedtest"


def nested_get(d, *keys):
    """
    A safe get function for a nested dictionary.
    """
    for key in keys:
        try:
            d = d[key]
        except KeyError:
            return None
        except TypeError:
            return None
    return d


def flatten_json(b, delim):
    """
    A simple function for flattening JSON by concatenating keys w/ a delimiter.
    """
    val = {}
    for i in b.keys():
        if isinstance(b[i], dict):
            get = flatten_json(b[i], delim)
            for j in get.keys():
                val[i + delim + j] = get[j]
        else:
            val[i] = b[i]

    return val


def extract_pattern(string, template):
    """
    A rudimentary function for extracting patterns from a string by comparing
    differences between it and a template string.
    """
    output = {}
    entry = ""
    value = ""

    for s in difflib.ndiff(string, template):
        if s[0] == " ":
            if entry != "" and value != "":
                output[entry] = value
                entry = ""
                value = ""
        elif s[0] == "-":
            value += s[2]
        elif s[0] == "+":
            if s[2] != "%":
                entry += s[2]

    # check ending if non-empty
    if entry != "" and value != "":
        output[entry] = value

    return output


def import_speedtest(path):
    """
    Import function for Speedtest.net tests.
    """
    with jsonlines.open(path, mode="r") as reader:
        line = reader.read()
        return flatten_json(line, "_")


def import_dash_legacy(path):
    """
    Import function for legacy-format DASH tests..
    """
    record = {}
    with jsonlines.open(path, mode="r") as reader:
        try:
            data = reader.read(skip_empty=True)
        except EOFError:
            return {}
        if "test_name" in data:
            record["probe_asn"] = nested_get(data, "probe_asn")
            record["probe_cc"] = nested_get(data, "probe_cc")
            record["connect_latency"] = nested_get(data, "test_keys", "simple",
                                                   "connect_latency")
            record["median_bitrate"] = nested_get(data, "test_keys", "simple",
                                                  "median_bitrate")
            record["min_playout_delay"] = nested_get(data, "test_keys",
                                                     "simple",
                                                     "min_playout_delay")
            record["test_name"] = nested_get(data, "test_name")
            record["test_runtime"] = nested_get(data, "test_runtime")
            record["test_start_time"] = nested_get(data, "test_start_time")
            return record


def import_ndt_legacy(path):
    """
    Import function for legacy-format NDT tests..
    """
    record = {}
    with jsonlines.open(path, mode="r") as reader:
        try:
            data = reader.read(skip_empty=True)
        except EOFError:
            return {}
        if "test_keys" in data:
            record["probe_asn"] = nested_get(data, "probe_asn")
            record["probe_cc"] = nested_get(data, "probe_cc")
            record["software_name"] = nested_get(data, "software_name")
            record["server_address"] = nested_get(data, "test_keys",
                                                  "server_address")
            record["avg_rtt"] = nested_get(data, "test_keys", "advanced",
                                           "avg_rtt")
            record["congestion_limited"] = nested_get(data, "test_keys",
                                                      "advanced",
                                                      "congestion_limited")
            record["max_rtt"] = nested_get(data, "test_keys", "advanced",
                                           "max_rtt")
            record["min_rtt"] = nested_get(data, "test_keys", "advanced",
                                           "min_rtt")
            record["packet_loss"] = nested_get(data, "test_keys", "advanced",
                                               "packet_loss")
            record["receiver_limited"] = nested_get(data, "test_keys",
                                                    "advanced",
                                                    "receiver_limited")
            record["sender_limited"] = nested_get(data, "test_keys",
                                                  "advanced", "sender_limited")
            record["download"] = nested_get(data, "test_keys", "simple",
                                            "download")
            record["ping"] = nested_get(data, "test_keys", "simple", "ping")
            record["upload"] = nested_get(data, "test_keys", "simple",
                                          "upload")
            record["test_runtime"] = nested_get(data, "test_runtime")
            record["test_start_time"] = nested_get(data, "test_start_time")
            return record


tests = {
    "speedtest": import_speedtest,
    "dash_legacy": import_dash_legacy,
    "ndt_legacy": import_ndt_legacy,
}


def export_csv(path, data):
    """
    Export function for CSV-format output files.
    """
    data.sort(key=lambda d: len(d), reverse=True)
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        return writer.writerows(data)


exporters = {"csv": export_csv}


def main():
    """ The main function for the converter script."""
    parser = configargparse.ArgParser(
        auto_env_var_prefix="murakami_convert_",
        description="A Murakami test output file format parser.",
        ignore_unknown_config_file_keys=False,
    )
    parser.add(
        "-l",
        "--loglevel",
        dest="loglevel",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add(
        "-f",
        "--format",
        dest="format",
        default=DEFAULT_FORMAT,
        choices=exporters.keys(),
        help="Set the output format.",
    )
    parser.add(
        "-t",
        "--test",
        dest="test",
        required=True,
        choices=tests.keys(),
        help="The type of test data that is being parsed.",
    )
    parser.add(
        "-o",
        "--output",
        required=True,
        dest="output",
        help="Path to output file.",
    )
    parser.add(
        "-p",
        "--pattern",
        dest="pattern",
        help=
        "An input filename pattern containing one or more of %%l (location type), %%n (network type), %%c (connection type), and %%d (datestamp).",
    )
    parser.add(
        "input",
        nargs="+",
        help=
        "The input filename, directory, or pattern containing test results.",
    )
    settings = parser.parse_args()

    logging.basicConfig(
        level=settings.loglevel,
        format="%(asctime)s %(filename)s:%(lineno)s %(levelname)s %(message)s",
    )

    pathlist = []
    for i in settings.input:
        pathlist.append(glob.glob(i))

    pathlist = list(set().union(*pathlist))

    if not pathlist:
        logger.error("No valid files found in specified path.")
        sys.exit(1)

    records = []
    for path in pathlist:
        if os.path.isdir(path):
            continue
        importer = tests.get(settings.test, DEFAULT_TEST)
        contents = importer(path)
        if settings.pattern:
            pattern = extract_pattern(os.path.basename(path), settings.pattern)
            if "l" in pattern:
                contents["location"] = pattern["l"]
            if "n" in pattern:
                contents["network_type"] = pattern["n"]
            if "c" in pattern:
                contents["connection_type"] = pattern["c"]
            if "d" in pattern:
                contents["datestamp"] = pattern["d"]
        records.append(contents)

    if not records:
        logger.error("No valid records found in specified files.")
        sys.exit(1)

    exporter = exporters.get(settings.format, DEFAULT_FORMAT)
    exporter(settings.output, records)
