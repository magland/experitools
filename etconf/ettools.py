from copy import deepcopy
import time
import os
import stat
from typing import Optional, List

class ETConf:
    def __init__(self, *, defaults, config_dir, preset_config_url):
        self._defaults = defaults
        self._config_dir = config_dir
        self._config = deepcopy(self._defaults)
        self._preset_config = None
        self._preset_config_url = None
    def set_config(self, preset=None, **kwargs):
        if preset is not None:
            self._load_preset_config_if_needed()
            x = self._preset_config['configurations'][preset]
            for k, v in x.items():
                self._config[k] = deepcopy(v)
        for k, v in kwargs.items():
            if v is not None:
                self._config[k] = deepcopy(v)
    def get_config(self):
        return deepcopy(self._config)
    def _load_preset_config_if_needed(self):
        if self._preset_config is not None:
            return
        if not os.path.exists(self._config_dir):
            os.mkdir(self._config_dir)
        config_path = os.path.join(self._config_dir, 'preset_configuration.json')
        try_download = True
        if os.path.exists(config_path):
            obj = None
            try:
                obj0 = _read_json_file(config_path)
                if obj0 and obj0.get('configurations'):
                    obj = obj0
            except:
                pass
            if obj is not None and _file_age_sec(config_path) <= 60:
                try_download = False
        if try_download and self._preset_config_url:
            url = self._load_preset_config_if_needed
            try:
                obj0 = _http_get_json(url)
                if obj0 and obj0.get('configurations', None):
                    obj = obj0
                    _write_json_file(obj, config_path)
                else:
                    print(obj0.get('error', ''))
                    print('Warning: Problem loading preset configurations from: {}'.format(url))
            except:
                print('Warning: unable to load preset configurations from: {}'.format(url))
        if obj is None:
            raise Exception('Unable to load preset configurations')
        self._preset_config = obj

def _file_age_sec(pathname):
    return time.time() - os.stat(pathname)[stat.ST_MTIME]

_http_get_cache = dict()
def _http_get_json(url: str, use_cache_on_success: bool=False, verbose: Optional[bool]=False, retry_delays: Optional[List[float]]=None) -> dict:
    if use_cache_on_success:
        if url in _http_get_cache:
            return _http_get_cache[url]
    timer = time.time()
    if retry_delays is None:
        retry_delays = [0.2, 0.5]
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_get_json::: ' + url)
    try:
        req = request.urlopen(url)
    except:
        if len(retry_delays) > 0:
            print('Retrying http request in {} sec: {}'.format(
                retry_delays[0], url))
            time.sleep(retry_delays[0])
            return _http_get_json(url, verbose=verbose, retry_delays=retry_delays[1:])
        else:
            return dict(success=False, error='Unable to open url: ' + url)
    try:
        ret = json.load(req)
    except:
        return dict(success=False, error='Unable to load json from url: ' + url)
    if verbose:
        print('Elapsed time for _http_get_json: {} {}'.format(time.time() - timer, url))
    if use_cache_on_success:
        if ret['success']:
            _http_get_cache[url] = ret
    return ret