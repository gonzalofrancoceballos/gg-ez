import urllib
from  http.client import HTTPResponse
from abc import abstractmethod


class ApiConnector:
    @abstractmethod
    def read(self, path, mode):
        pass


class RapidApiConnector(ApiConnector):
    def __init__(self, api_key):
        self._api_key = api_key
        self._base_url = "https://api-football-v1.p.rapidapi.com/v2"
        self._headers = {
            "x-rapidapi-host": "api-football-v1.p.rapidapi.com",
            "x-rapidapi-key": api_key,
            "useQueryString": True
        }

    def read(self, path="", mode=None) -> HTTPResponse:
        url = "/".join([self._base_url, path])
        req = urllib.request.Request(url, headers=self._headers)
        response = urllib.request.urlopen(req)
        return response
