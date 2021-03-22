from time import sleep
from urllib.parse import urlencode
from datetime import datetime, time, timedelta, date
import requests
import json


class Alpaca:
    def __init__(self, client_id=None, client_secret=None):
        # print("Inside Init")
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = "https://data.alpaca.markets"
        self.limit = 10000
        self.counter = 0
        self.fl_path = "" // <FilePath>

    def _set_dates(self):
        self._yesterday = date.today() - timedelta(days=16)
        fmt_date = "%Y-%m-%dT%H:%M:%SZ"
        self._start_time = time(hour=9, minute=0, second=0)
        self._end_time = time(hour=20, minute=0, second=0)

        self.start = datetime.combine(self._yesterday, self._start_time).strftime(fmt_date)
        self.end = datetime.combine(self._yesterday, self._end_time).strftime(fmt_date)
        return self.start, self.end

    def _build_url(self, base_url, *res, **params):
        url = base_url
        for r in res:
            url = '{}/{}'.format(url, r)
        if params:
            url = '{}?{}'.format(url, urlencode(params))
        return url

    def _headers(self):
        return {
            'APCA-API-KEY-ID': self.client_id,
            'APCA-API-SECRET-KEY': self.client_secret
        }

    def _params(self, market):
        endpoint = f"v2/stocks/AAPL/{market}"
        start_time, end_time = self._set_dates()
        limit = self.limit
        interval = "1Hour"

        if market == "bars":
            return endpoint, {"start": start_time, "end": end_time, "limit": limit, "timeframe": interval}
        else:
            return endpoint, {"start": start_time, "end": end_time, "limit": limit}

    def _get_url(self, market):
        endpoint, param = self._params(market=market)
        url = self._build_url(self.url, endpoint, **param)
        return url

    def _create(self, data, file):
        dt = self._yesterday.strftime("%Y%m%d")
        file = f"{self.fl_path}/{dt}/{file}"
        try:
            with open(file, 'a') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except FileNotFoundError as er:
            raise Exception("Could not find directory to create the file")
        return

    def _connect(self, url, market):
        # print("Inside connect")
        res = requests.get(url=url, headers=self._headers())
        print(f"Connected to {res.url}")
        self.counter += 1
        while self.counter:
            if res.status_code == 500:
                print("Internal Server Error: ", self.counter)
                break
            if res.status_code not in range(200, 299):
                print(res.status_code)
                print(res.content)
                raise Exception("Url not accessible")
            next_page = res.json()
            self._create(data=res.json(), file=f"{market}_AAPL.json")
            token = next_page["next_page_token"]
            if not token:
                print("All Pages Read", self.counter)
                self.counter = 0
                break
            if self.counter % 200 == 0:
                sleep(60)
            next_page = self._get_url(market=market) + f"&page_token={token}"
            print("Getting Page: ", self.counter, " Page Token : ", token)
            self._connect(url=next_page, market=market)
        return res.json()

    def _template(self, market=None):
        if market is None:
            raise Exception(f"valid market is required.")
        print(f"Getting {market} data")
        endpoint, param = self._params(market=market)
        url = self._build_url(self.url, endpoint, **param)
        return self._connect(url=url, market=market)

    def _tickers(self, in_file="stocks.json"):
        """
        Accepts the Stock List created on stk_mkt_ref_data.
        File Format should be List of JSON Objects
        :param file_name:
        :return: List of Symbols that is used to download dividends / financials
        """
        with open(in_file, "r") as f:
            all_tickers = json.load(f)

        # ticker_list = [val["ticker"] for stock in all_tickers for val in stock["results"]]
        ticker_list = [stock["ticker"] for stock in all_tickers]
        return ticker_list

    def trades(self):
        return self._template(market="trades")

    def bars(self):
        self._template(market="bars")

    def quotes(self):
        self._template(market="quotes")
