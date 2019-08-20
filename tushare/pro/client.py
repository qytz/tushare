# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pro数据接口 
Created on 2017/07/01
@author: polo,Jimmy
@group : tushare.pro
"""

import pandas as pd
import simplejson as json
from functools import partial
import requests

try:
    import aiohttp
except ImportError:
    aiohttp = None


class DataApi:

    __token = ''
    __http_url = 'http://api.tushare.pro'

    def __init__(self, token, timeout=10):
        """
        Parameters
        ----------
        token: str
            API接口TOKEN，用于用户认证
        """
        self.__token = token
        self.__timeout = timeout

    def query(self, api_name, fields='', **kwargs):
        req_params = {
            'api_name': api_name,
            'token': self.__token,
            'params': kwargs,
            'fields': fields
        }

        res = requests.post(self.__http_url, json=req_params, timeout=self.__timeout)
        result = json.loads(res.text)
        if result['code'] != 0:
            raise Exception(result['msg'])
        data = result['data']
        columns = data['fields']
        items = data['items']

        return pd.DataFrame(items, columns=columns)

    def __getattr__(self, name):
        return partial(self.query, name)


if aiohttp:
    class AsyncDataApi:

        __token = ''
        __http_url = 'http://api.tushare.pro'

        def __init__(self, token, timeout=10, session=None):
            """
            Parameters
            ----------
            token: str
                API 接口 TOKEN，用于用户认证
            """
            self.__token = token
            self.__timeout = timeout
            self.__http_session = session

        async def query(self, api_name, fields="", **kwargs):
            req_params = {
                "api_name": api_name,
                "token": self._token,
                "params": kwargs,
                "fields": fields,
            }

            if not self.__http_session:
                self.__http_session = aiohttp.ClientSession(timeout=self.__timeout)

            res = await self.__http_session.post(
                self._http_url, json=req_params
            )
            result = await res.json()
            if result["code"] != 0:
                raise Exception(result["msg"])
            data = result["data"]
            columns = data["fields"]
            items = data["items"]

            return pd.DataFrame(items, columns=columns)

        def __getattr__(self, name):
            return partial(self.query, name)

        async def close(self) -> None:
            if self.__http_session:
                await self.__http_session.close()

        @property
        def closed(self) -> bool:
            """Is client session closed.

            A readonly property.
            """
            return self.__http_session.closed

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self.close()
