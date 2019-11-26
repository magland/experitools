import shutil
import os
import tempfile
import sys
from functools import wraps
from typing import Union, List
import json
import kachery as ka
import pairio as pa
import loggery
import time

from .consolecapture import ConsoleCapture

def function(name, version):
    def wrap(f):
        def execute(_force_run=False, **kwargs):
            hash_object = dict(
                api_version='0.1.0',
                name=name,
                version=version,
                input_files=dict(),
                output_files=dict(),
                parameters=dict()
            )
            resolved_kwargs = dict()
            hither_input_files = getattr(f, '_hither_input_files', [])
            hither_output_files = getattr(f, '_hither_output_files', [])
            hither_parameters = getattr(f, '_hither_parameters', [])
            for input_file in hither_input_files:
                iname = input_file['name']
                if iname not in kwargs or kwargs[iname] is None:
                    if input_file['required']:
                        raise Exception('Missing required input file: {}'.format(iname))
                else:
                    x = kwargs[iname]
                    if type(x) == str and not _is_hash_url(x):
                        # path to a local file
                        pass
                    else:
                        if isinstance(x, File):
                            # a hither File object
                            if x._path is None:
                                raise Exception('Input file has no path: {}'.format(iname))
                            # we really want the path
                            x = x._path
                        if _is_hash_url(x):
                            # a hash url
                            y = ka.load_file(x)
                            if y is None:
                                raise Exception('Unable to load input file {}: {}'.format(iname, x))
                            x = y
                    info0 = ka.get_file_info(x)
                    if info0 is None:
                        raise Exception('Unable to get info for file intput file {}: {}'.format(iname, x))
                    tmp0 = dict()
                    for field0 in ['sha1', 'md5']:
                        if field0 in info0:
                            tmp0[field0] = info0[field0]
                    hash_object['input_files'][iname] = tmp0
                    resolved_kwargs[iname] = x
            for output_file in hither_output_files:
                oname = output_file['name']
                if oname not in kwargs or kwargs[oname] is None:
                    if output_file['required']:
                        raise Exception('Missing required output file: {}'.format(oname))
                else:
                    x = kwargs[oname]
                    if type(x) == str and _is_hash_url(x):
                        raise Exception('Output file {} cannot be a hash URI: {}'.format(oname, x))
                    if type(x) == str:
                        resolved_kwargs[oname] = x
                    elif type(x) == bool:
                        if x == True:
                            resolved_kwargs[oname] = _make_temporary_file('kachery_output_file_')
                        else:
                            pass
                    else:
                        raise Exception('Unexpected type')
                    if oname in resolved_kwargs:
                        hash_object['output_files'][oname] = True
            for parameter in hither_parameters:
                pname = parameter['name']
                if pname not in kwargs or kwargs[pname] is None:
                    if parameter['required']:
                        raise Exception('Missing required parameter: {}'.format(pname))
                    if 'default' in parameter:
                        resolved_kwargs[pname] = parameter['default']
                else:
                    resolved_kwargs[pname] = kwargs[pname]
                hash_object['parameters'][pname] = resolved_kwargs[pname]
            for k, v in kwargs.items():
                if k not in resolved_kwargs:
                    hash_object['parameters'][k] = v
                    resolved_kwargs[k] = v
            if not _force_run:
                result_serialized: Union[dict, None] = _load_result(hash_object=hash_object)
                if result_serialized is not None:
                    result0 = _deserialize_result(result_serialized)
                    if result0 is not None:
                        if result0.runtime_info['stdout']:
                            sys.stdout.write(result0.runtime_info['stdout'])
                        if result0.runtime_info['stderr']:
                            sys.stderr.write(result0.runtime_info['stderr'])
                        print('===== Hither: using cached result for {}'.format(name))
                        return result0
            with ConsoleCapture() as cc:
                returnval = f(**resolved_kwargs)
            result = Result()
            result.outputs = Outputs()
            for oname in hash_object['output_files'].keys():
                setattr(result.outputs, oname, resolved_kwargs[k])
                result._output_names.append(oname)
            result.runtime_info = cc.runtime_info()
            result.hash_object = hash_object
            result.retval = returnval
            _store_result(serialized_result=_serialize_result(result))
            return result
        setattr(f, 'execute', execute)
        return f
    return wrap

def input_file(name: str, required=True):
    def wrap(f):
        hither_input_files = getattr(f, '_hither_input_files', [])
        hither_input_files.append(dict(
            name=name,
            required=required
        ))
        setattr(f, '_hither_input_files', hither_input_files)
        return f
    return wrap

def output_file(name: str, required=True):
    def wrap(f):
        hither_output_files = getattr(f, '_hither_output_files', [])
        hither_output_files.append(dict(
            name=name,
            required=required
        ))
        setattr(f, '_hither_output_files', hither_output_files)
        return f
    return wrap

def parameter(name: str, required=True, default=None):
    def wrap(f):
        hither_parameters = getattr(f, '_hither_parameters', [])
        hither_parameters.append(dict(
            name=name,
            required=required,
            default=default
        ))
        setattr(f, '_hither_parameters', hither_parameters)
        return f
    return wrap

def _load_result(*, hash_object):
    name0 = 'hither_result'
    hash0 = ka.get_object_hash(hash_object)
    doc = loggery.find_one({'message.name': name0, 'message.hash': hash0})
    if doc is None:
        return None
    return doc['message']

def _store_result(*, serialized_result):
    loggery.insert_one(message=serialized_result)

class Result():
    def __init__(self):
        self.hash_object = None
        self.runtime_info = None
        self.retval = None
        self.outputs = Outputs()
        self._output_names = []

def _serialize_result(result):
    ret = dict(
        output_files=dict()
    )
    ret['name'] = 'hither_result'

    ret['runtime_info'] = result.runtime_info
    ret['runtime_info']['stdout'] = ka.store_text(ret['runtime_info']['stdout'])
    ret['runtime_info']['stderr'] = ka.store_text(ret['runtime_info']['stderr'])

    for oname in result._output_names:
        ret['output_files'][oname] = ka.store_file(getattr(result.outputs, oname))

    ret['retval'] = result.retval
    ret['hash_object'] = result.hash_object
    ret['hash'] = ka.get_object_hash(result.hash_object)
    return ret

def _deserialize_result(obj):
    result = Result()
    
    result.runtime_info = obj['runtime_info']
    result.runtime_info['stdout'] = ka.load_text(result.runtime_info['stdout'])
    result.runtime_info['stderr'] = ka.load_text(result.runtime_info['stderr'])
    if result.runtime_info['stdout'] is None:
        return None
    if result.runtime_info['stderr'] is None:
        return None
    
    output_files = obj['output_files']
    for oname, path in output_files.items():
        path2 = ka.load_file(path)
        if path2 is None:
            return None
        setattr(result.outputs, oname, path2)
        result._output_names.append(oname)
    
    result.retval = obj['retval']
    result.hash_object = obj['hash_object']
    return result


class Outputs():
    def __init__(self):
        pass


class File():
    def __init__(self, path: Union[str, None]=None):
        self._path = path
    def __str__(self):
        if self._path is not None:
            return 'hither.File({})'.format(self._path)
        else:
            return 'hither.File()'

def _is_hash_url(path):
    algs = ['sha1', 'md5']
    for alg in algs:
        if path.startswith(alg + '://') or path.startswith(alg + 'dir://'):
            return True
    return False

def _make_temporary_file(prefix):
    with tempfile.NamedTemporaryFile(prefix=prefix, delete=False) as tmpfile:
        temp_file_name = tmpfile.name
    return temp_file_name