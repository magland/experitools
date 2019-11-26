import os
from typing import Union, Tuple, Optional, List, Dict
import json
import hashlib
import time
import pymongo
from etconf import ETConf

class _MongoClient:
    def __init__(self):
        self._url = None
        self._client = None
    def insert_one(self, *, message, config):
        db = self._get_db(config)
        if not db:
            return None
        doc = dict(
            time=time.time() - 0,
            message=message
        )
        db.insert_one(doc)
    def find_one(self, *, query, config):
        db = self._get_db(config)
        if not db:
            return None
        for doc in db.find(query).sort('time', direction=pymongo.DESCENDING):
            return doc
    def _get_db(self, config):
        if not config['url']:
            return None
        if config['url'] != self._url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(config['url'], retryWrites=False)
            self._url = config['url']
        return self._client['loggery'][config['collection']]

_global_client = _MongoClient()

_global_config = ETConf(
    defaults=dict(
        url=None,
        collection='default',
        verbose=False
    ),
    config_dir=os.path.join(os.path.expanduser("~"), '.loggery'),
    preset_config_url='https://raw.githubusercontent.com/magland/experitools/config/config/config_2019a.json'
)

def set_config(*,
        url: Union[str, None]=None,
        collection: Union[str, None]=None,
        verbose: Union[bool, None]=None
) -> None:
    _global_config.set_config(url=url, collection=collection, verbose=verbose)

def insert_one(message: dict):
    return _global_client.insert_one(message=message, config=_global_config.get_config())

def find_one(query):
    return _global_client.find_one(query=query, config=_global_config.get_config())

