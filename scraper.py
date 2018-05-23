import pandas
import time
import datetime
import io
import requests
import pprint

from bs4 import BeautifulSoup
from pymongo import MongoClient

class Scraper:
    def __init__(self, test=False):
        if test:
            address='localhost'
            port=27017
            db='mlbDB'
            self._client = MongoClient(address, port)
        else:
            url="mongodb://alex:Q8b5^SR5Oh@ds123110-a0.mlab.com:23110,ds123110-a1.mlab.com:23110/heroku_kcpx1gp1?replicaSet=rs-ds123110"
            db='heroku_kcpx1gp1'
            self._client = MongoClient(url)
        self._db = self._client[db]
        self._current_year = datetime.date.today().strftime('%Y')
        self._current_day  = datetime.date.today().strftime('%Y-%m-%d')

    def _set_cache(self, url, dfs, data_key, ttl):
        if ttl > 0:
            expire_time = time.time() + ttl
        else:
            expire_time = ttl # -1 for forever, 0 for always replace

        contents = {}
        for key, df in dfs.items():
            contents[key] = df.to_csv()

        self._db.ScraperCache.insert_one(
            {'url' : url, 
             'key' : data_key,
             'contents' : contents,
             'expires' : expire_time})

    def _get_cache(self, url):
        entry = self._db.ScraperCache.find_one({"url" : url})
        if entry is not None:
            if entry['expires'] == -1 or entry['expires'] > time.time():
                dfs = {}
                for key, csv in entry['contents'].items():
                    dfs[key] = pandas.read_csv(io.StringIO(csv))

                return dfs
            else:
                return None

    def get_key(self, data_key):
        entry = self._db.ScraperCache.find_one({"key" : data_key})
        if entry is not None:
            if entry['expires'] <= 0 or entry['expires'] > time.time():
                dfs = {}
                for key, csv in entry['contents'].items():
                    dfs[key] = pandas.read_csv(io.StringIO(csv))
                return dfs
            else:
                return None

    def scrape(self, url, data_key, module, handler, ttl):
        dfs = self._get_cache(url)
        if dfs is not None:
            return dfs
        else:
            page = requests.get(url).text
            parser = BeautifulSoup(page, "html.parser")
            mod = __import__(module)
            method_to_call = getattr(mod, handler)
            dfs = method_to_call(parser)

            if dfs is not None:
                self._set_cache(url, dfs, data_key, ttl)


