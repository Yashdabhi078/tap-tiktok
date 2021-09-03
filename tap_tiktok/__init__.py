#!/usr/bin/env python3
import os
import json
import backoff
import requests
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse

REQUIRED_CONFIG_KEYS = ["advertiser_id", "report_type", "data_level", "dimensions", "start_date", "end_date", "token"]
LOGGER = singer.get_logger()
HOST = "ads.tiktok.com"
PATH = "/open_api/v1.2/reports/integrated/get"


class TiktokError(Exception):
    def __init__(self, msg, code):
        self.msg = msg
        self.code = code
        super().__init__(self.msg)


def giveup(exc):
    """
    code 40100 shows rate limit reach error
    it will give up on retry operation, if code is not 40100
    """
    return exc.code != 40100


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", HOST
    return urlunparse((scheme, netloc, path, "", query, ""))


def create_metadata_for_report(schema):
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available"}}]
    for key in schema.properties:
        # hence when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            inclusion = "available" if key != "dimensions" else "automatic"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        else:
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": "available"}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema)
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


@backoff.on_exception(backoff.expo, TiktokError, max_tries=5, giveup=giveup, factor=2)
@utils.ratelimit(10, 1)
def make_request(url, headers):
    response = requests.get(url, headers=headers)
    code = response.json().get("code")
    if code != 0:
        LOGGER.error('Return Code = %s', code)
        raise TiktokError(response.json().get("message", "an error occurred while calling API"), code)

    return response


def request_data(config, state, stream):
    page = 1
    total_page = 1
    all_items = []
    key = stream.replication_key if stream.replication_key else "stat_time_day"
    start_date = singer.get_bookmark(state, stream.tap_stream_id, key).split(" ")[0] if state else str(config["start_date"])
    lifetime = stream.replication_method is not "INCREMENTAL"

    # "stat_time_day" is unsupported dimensions when lifetime is true
    if lifetime and "stat_time_day" in config["dimensions"]:
        config["dimensions"].remove("stat_time_day")
        # TODO: remove "stat_time_hour" if we use it as dimension

    headers = {"Access-Token": config["token"]}
    attr = {
        "advertiser_id": config["advertiser_id"],
        "report_type": config["report_type"],
        "data_level": config["data_level"],
        "dimensions": config["dimensions"],
        "start_date": start_date,
        "end_date": str(config["end_date"]),
        "lifetime": lifetime,
        "page_size": 200
    }

    # do pagination
    while page <= total_page:
        attr["page"] = page

        query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in attr.items()})
        url = build_url(PATH, query_string)
        response = make_request(url, headers=headers)

        data = response.json().get("data", {})
        all_items += data.get("list", [])
        LOGGER.info("Retrieved page --> %d", page)

        page = data.get("page_info", {}).get("page", 1) + 1
        total_page = data.get("page_info", {}).get("total_page", 1)
    return all_items


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key if stream.replication_key else "stat_time_day"
        is_sorted = False

        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        tap_data = request_data(config, state, stream)

        max_bookmark = None
        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in tap_data:
                # Type Conversation and Transformation
                transformed_data = transform(row, schema, metadata=mdata)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [transformed_data])
                if bookmark_column:
                    if is_sorted:
                        # update bookmark to latest value
                        max_bookmark = row["dimensions"][bookmark_column]
                        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, max_bookmark)
                        singer.write_state(state)
                    else:
                        # if data unsorted, save max value until end of writes
                        max_bookmark = row["dimensions"][bookmark_column] if max_bookmark is None else max_bookmark
                        max_bookmark = max(max_bookmark, row["dimensions"][bookmark_column])
            counter.increment(len(tap_data))
        if bookmark_column and not is_sorted and max_bookmark:
            state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, max_bookmark)
            singer.write_state(state)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
