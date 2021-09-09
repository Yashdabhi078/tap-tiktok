import json
from unittest.mock import patch
import pytest
from singer.catalog import Catalog
from tap_tiktok import sync

CONFIG = {'start_date': '2021-09-06', 'end_date': '2021-09-06', 'advertiser_id': '6998333412931026945',
          'report_type': 'BASIC', 'data_level': 'AUCTION_ADGROUP', 'dimensions': ['adgroup_id', 'stat_time_day'],
          'token': '8deae09ca4fa3c56092384a5a2f6f4b7946b6418', 'disable_collection': True}
STATE = {}
CATALOG = {"streams": [{"tap_stream_id": "ad_id_report", "key_properties": [], "schema": {"properties": {"dimensions": {
    "properties": {"stat_time_day": {"format": "date-time", "type": ["null", "string"]},
                   "ad_id": {"type": ["null", "string"]}}, "type": ["null", "object"]}, "metrics": {
    "properties": {"ad_name": {"type": ["null", "string"]}, "spend": {"type": ["null", "string"]},
                   "impressions": {"type": ["null", "string"]}, "clicks": {"type": ["null", "string"]},
                   "ctr": {"type": ["null", "string"]}}, "type": ["null", "object"]}}, "type": ["null", "object"]},
                        "stream": "ad_id_report",
                        "metadata": [{"breadcrumb": [], "metadata": {"inclusion": "available", "selected": True}},
                                     {"breadcrumb": ["properties", "dimensions", "properties", "stat_time_day"],
                                      "metadata": {"inclusion": "automatic", "selected": True}},
                                     {"breadcrumb": ["properties", "dimensions", "properties", "ad_id"],
                                      "metadata": {"inclusion": "automatic", "selected": True}},
                                     {"breadcrumb": ["properties", "metrics", "properties", "ad_name"],
                                      "metadata": {"inclusion": "available", "selected": True}},
                                     {"breadcrumb": ["properties", "metrics", "properties", "spend"],
                                      "metadata": {"inclusion": "available", "selected": False}},
                                     {"breadcrumb": ["properties", "metrics", "properties", "impressions"],
                                      "metadata": {"inclusion": "available", "selected": True}},
                                     {"breadcrumb": ["properties", "metrics", "properties", "clicks"],
                                      "metadata": {"inclusion": "available", "selected": False}},
                                     {"breadcrumb": ["properties", "metrics", "properties", "ctr"],
                                      "metadata": {"inclusion": "available", "selected": True}}]}]}

RESPONSE = {'message': 'OK', 'code': 0,
            'data': {'page_info': {'total_number': 0, 'page': 1, 'page_size': 200, 'total_page': 0},
                     'list': [{"metrics": {"ad_name": "yash20200923012039", "clicks": "116.0", "spend": "76.73",
                                           "impressions": "10505.0", "ctr": "1.1"},
                               "dimensions": {"stat_time_day": "2020-10-17 00:00:00", "ad_id": 1678604629756978}},
                              {"metrics": {"ad_name": "deep20200923012039", "clicks": "134.0", "spend": "106.4",
                                           "impressions": "12003.0", "ctr": "1.12"},
                               "dimensions": {"stat_time_day": "2020-10-16 00:00:00", "ad_id": 1678604629756978}}]},
            'request_id': '202109091045220102450452132E18FEEB'}

OUTCOME = [
    {
        "type": "RECORD", "stream": "ad_id_report",
        "record": {
            "metrics": {"ad_name": "yash20200923012039", "impressions": "10505.0", "ctr": "1.1"},
            "dimensions": {"stat_time_day": "2020-10-17T00:00:00.000000Z", "ad_id": "1678604629756978"}
        }},
    {
        "type": "RECORD", "stream": "ad_id_report",
        "record": {
            "metrics": {"ad_name": "deep20200923012039", "impressions": "12003.0", "ctr": "1.12"},
            "dimensions": {"stat_time_day": "2020-10-16T00:00:00.000000Z", "ad_id": "1678604629756978"}
        }}
]


@pytest.mark.parametrize("config, state, catalog, response, outcome",
                         [(CONFIG, STATE, CATALOG, RESPONSE, OUTCOME)])
def test_sync_records(config, state, catalog, response, outcome, capsys):
    """
    will check that, only selected metrics in catalog metadata should be exported for records.
    """
    catalog = Catalog.from_dict(catalog)
    with patch('tap_tiktok.requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = response

        sync(config, state, catalog)
        captured = capsys.readouterr().out
        list_outputs = [json.loads(out) for out in captured.split("\n") if out != ""]

        for output in list_outputs:
            if output.get("type") == "RECORD":
                assert output in outcome
