#!/usr/bin/env python3
import os
import json
from collections import namedtuple

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
        mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": "available"}})
        if "object" in schema.properties.get(key).type:
            inclusion = "available" if key != "dimensions" else "automatic"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])

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


def request_data(config, stream):
    page = 1
    total_page = 1
    all_items = []

    headers = {"Access-Token": config.token}
    attr = {
        "advertiser_id": config.advertiser_id,
        "report_type": config.report_type,
        "data_level": config.data_level,
        "dimensions": config.dimensions,
        "start_date": str(config.start_date),
        "end_date": str(config.end_date),
        "lifetime": stream.replication_method is not "INCREMENTAL",
        "page_size": 200
    }

    # do pagination
    while page <= total_page:
        attr["page"] = page

        query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in attr.items()})
        url = build_url(PATH, query_string)
        response = requests.get(url, headers=headers)

        data = response.json().get("data", {})
        all_items += data.get("list", [])

        page = data.get("page_info", {}).get("page", 1) + 1
        total_page = data.get("page_info", {}).get("total_page", 1)

    return all_items


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        tap_data = request_data(config, stream)

        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in tap_data:
                # Type Conversation and Transformation
                transformed_data = transform(row, schema, metadata=mdata)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [transformed_data])
            counter.increment(len(tap_data))
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
