#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import json
import logging
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
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Length": "110",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "JSESSIONID=0000CX6KiWeduTAqThpQ13j8nBs:-1",
            "Host": "srh.bankofchina.com",
            "Origin": "https://srh.bankofchina.com",
            "Pragma": "no-cache",
            "Referer": "https://srh.bankofchina.com/search/whpj/search_cn.jsp",
            "sec-ch-ua": '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
        }

    def request_body_data(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Mapping, str]]:
        cur_map = currency_map.CURRENCY_MAP
        return {
            "erectDate": "2022-09-13",
            "nothing": "2022-09-13",
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
        m_n = html.xpath('//tr[{}]/td[1]/text()'.format(2))
        s_e_p = html.xpath('//tr[{}]/td[2]/text()'.format(2))
        c_p = html.xpath('//tr[{}]/td[3]/text()'.format(2))
        s_e_sp = html.xpath('//tr[{}]/td[4]/text()'.format(2))
        c_s = html.xpath('//tr[{}]/td[5]/text()'.format(2))
        bank_count_p = html.xpath('//tr[{}]/td[6]/text()'.format(2))
        date = html.xpath('//tr[{}]/td[7]/text()'.format(2))

        data = {
            "currency": self.parse_data(m_n[0]),
            "spot_purchase_price": self.parse_data(s_e_p[0]),
            "cash_purchase_price": self.parse_data(c_p[0]),
            "spot_sell_price": self.parse_data(s_e_sp[0]),
            "cash_selling_price": self.parse_data(c_s[0]),
            "bank_count_price": self.parse_data(bank_count_p[0]),
            "release_time": self.parse_data(date[0]),
        }
        return [data]
