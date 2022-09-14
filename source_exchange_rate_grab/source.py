#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import logging
import time
from typing import Any, List, Mapping, Tuple, Optional, Iterable, MutableMapping, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from lxml import etree

from . import currency_list
from . import currency_map

logger = logging.getLogger("airbyte")


class SourceExchangeRateGrab(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking Exchange Rate Grab API connection...")
        input_currency = config["currency"]
        if input_currency not in currency_list.CURRENCY_LIST:
            return False, f"Input Currency {input_currency} is invalid. Please check your spelling our input a valid Currency."
        else:
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [ExchangeRate(currency=config["currency"])]


class ExchangeRate(HttpStream):
    url_base = "https://srh.bankofchina.com/search/whpj/search_cn.jsp"
    http_method = "POST"

    primary_key = None

    def __init__(self, currency: str, **kwargs):
        super().__init__(**kwargs)
        self.currency = currency

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, **kwargs) -> str:
        return ""

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {}

    def request_body_data(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Mapping, str]]:
        cur_map = currency_map.CURRENCY_MAP
        current_time_str = time.strftime("%Y-%m-%d", time.localtime(time.time()))
        return {
            "erectDate": current_time_str,
            "nothing": current_time_str,
            "pjname": cur_map.get(self.currency),
            # "page": "2022-09-13",
            "head": "head_620.js",
            "bottom": "bottom_591.js",
        }

    def parse_data(self, input_str):
        res = input_str
        if '\r\n' in input_str:
            end_index = input_str.find('\r\n')
            res = input_str[:end_index]
        return res

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        html = etree.HTML(response.text)
        currency = html.xpath('//tr[{}]/td[1]/text()'.format(2))
        s_p_p = html.xpath('//tr[{}]/td[2]/text()'.format(2))
        c_p_p = html.xpath('//tr[{}]/td[3]/text()'.format(2))
        s_s_p = html.xpath('//tr[{}]/td[4]/text()'.format(2))
        c_s_p = html.xpath('//tr[{}]/td[5]/text()'.format(2))
        bank_count_p = html.xpath('//tr[{}]/td[6]/text()'.format(2))
        release_time = html.xpath('//tr[{}]/td[7]/text()'.format(2))

        data = {
            "currency": self.parse_data(currency[0]),
            "spot_purchase_price": self.parse_data(s_p_p[0]),
            "cash_purchase_price": self.parse_data(c_p_p[0]),
            "spot_sell_price": self.parse_data(s_s_p[0]),
            "cash_selling_price": self.parse_data(c_s_p[0]),
            "bank_count_price": self.parse_data(bank_count_p[0]),
            "release_time": self.parse_data(release_time[0]),
        }
        return [data]
