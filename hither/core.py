import shutil
import os
import tempfile
import sys
from functools import wraps
from typing import Union, List
import json
import time

from .consolecapture import ConsoleCapture
from .run_function_in_container import run_function_in_container

def function(name, version):
    def wrap(f):
        def execute(_force_run=False, _container=None, **kwargs):
            import kachery as ka
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

            # Let's make sure the input and output files are all coming in as File objects
            for input_file in hither_input_files:
                iname = input_file['name']
                if iname in kwargs:
                    if type(kwargs[iname]) == str:
                        kwargs[iname] = File(kwargs[iname])
            for output_file in hither_output_files:
                oname = output_file['name']
                if oname in kwargs:
                    if type(kwargs[oname]) == str:
                        kwargs[oname] = File(kwargs[oname])

            input_file_keys = []
            for input_file in hither_input_files:
                iname = input_file['name']
                if iname not in kwargs or kwargs[iname] is None:
                    if input_file['required']:
                        raise Exception('Missing required input file: {}'.format(iname))
                else:
                    x = kwargs[iname]
                    # a hither File object
                    if x._path is None:
                        raise Exception('Input file has no path: {}'.format(iname))
                    # we really want the path
                    x2 = x._path
                    if _is_hash_url(x2):
                        # a hash url
                        y = ka.load_file(x2)
                        if y is None:
                            raise Exception('Unable to load input file {}: {}'.format(iname, x))
                        x2 = y
                    info0 = ka.get_file_info(x2)
                    if info0 is None:
                        raise Exception('Unable to get info for input file {}: {}'.format(iname, x2))
                    tmp0 = dict()
                    for field0 in ['sha1', 'md5']:
                        if field0 in info0:
                            tmp0[field0] = info0[field0]
                    hash_object['input_files'][iname] = tmp0
                    input_file_keys.append(iname)
                    resolved_kwargs[iname] = x2
            
            output_file_keys = []
            for output_file in hither_output_files:
                oname = output_file['name']
                if oname not in kwargs or kwargs[oname] is None:
                    if output_file['required']:
                        raise Exception('Missing required output file: {}'.format(oname))
                else:
                    x = kwargs[oname]
                    x2 = x._path
                    if _is_hash_url(x2):
                        raise Exception('Output file {} cannot be a hash URI: {}'.format(oname, x2))
                    resolved_kwargs[oname] = x2
                    if oname in resolved_kwargs:
                        hash_object['output_files'][oname] = True
                        output_file_keys.append(oname)

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
                        for output_file in hither_output_files:
                            oname = output_file['name']
                            if oname in resolved_kwargs:
                                shutil.copyfile(getattr(result0.outputs, oname)._path, resolved_kwargs[oname])
                        _handle_temporary_outputs([getattr(result0.outputs, oname) for oname in output_file_keys])
                        if result0.runtime_info['stdout']:
                            sys.stdout.write(result0.runtime_info['stdout'])
                        if result0.runtime_info['stderr']:
                            sys.stderr.write(result0.runtime_info['stderr'])
                        print('===== Hither: using cached result for {}'.format(name))
                        return result0

            with ConsoleCapture() as cc:
                if _container is None:
                    returnval = f(**resolved_kwargs)
                else:
                    if hasattr(f, '_hither_containers'):
                        if _container in getattr(f, '_hither_containers'):
                            _container = getattr(f, '_hither_containers')[_container]
                    returnval = run_function_in_container(name=name, function=f, input_file_keys=input_file_keys, output_file_keys=output_file_keys, container=_container, keyword_args=resolved_kwargs)

            result = Result()
            result.outputs = Outputs()
            for oname in hash_object['output_files'].keys():
                setattr(result.outputs, oname, kwargs[k])
                result._output_names.append(oname)
            result.runtime_info = cc.runtime_info()
            result.hash_object = hash_object
            result.retval = returnval
            _handle_temporary_outputs([getattr(result.outputs, oname) for oname in output_file_keys])
            _store_result(serialized_result=_serialize_result(result))
            return result
        setattr(f, 'execute', execute)
        return f
    return wrap

def _handle_temporary_outputs(outputs):
    import kachery as ka
    for output in outputs:
        if output._is_temporary:
            old_path = output._path
            new_path = ka.load_file(ka.store_file(old_path))
            output._path = new_path
            output._is_temporary = False
            os.unlink(old_path)

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

def container(**kwargs):
    def wrap(f):
        if not hasattr(f, '_hither_containers'):
            setattr(f, '_hither_containers', dict())
        containers = getattr(f, '_hither_containers')
        for k, v in kwargs.items():
            containers[k] = v
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
    import kachery as ka
    import loggery
    name0 = 'hither_result'
    hash0 = ka.get_object_hash(hash_object)
    doc = loggery.find_one({'message.name': name0, 'message.hash': hash0})
    if doc is None:
        return None
    return doc['message']

def _store_result(*, serialized_result):
    import loggery
    loggery.insert_one(message=serialized_result)

class Result():
    def __init__(self):
        self.hash_object = None
        self.runtime_info = None
        self.retval = None
        self.outputs = Outputs()
        self._output_names = []

def _serialize_result(result):
    import kachery as ka
    ret = dict(
        output_files=dict()
    )
    ret['name'] = 'hither_result'

    ret['runtime_info'] = result.runtime_info
    ret['runtime_info']['stdout'] = ka.store_text(ret['runtime_info']['stdout'])
    ret['runtime_info']['stderr'] = ka.store_text(ret['runtime_info']['stderr'])

    for oname in result._output_names:
        path = getattr(result.outputs, oname)._path
        ret['output_files'][oname] = ka.store_file(path)

    ret['retval'] = result.retval
    ret['hash_object'] = result.hash_object
    ret['hash'] = ka.get_object_hash(result.hash_object)
    return ret

def _deserialize_result(obj):
    import kachery as ka
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
        setattr(result.outputs, oname, File(path2))
        result._output_names.append(oname)
    
    result.retval = obj['retval']
    result.hash_object = obj['hash_object']
    return result


class Outputs():
    def __init__(self):
        pass


class File():
    def __init__(self, path: Union[str, None]=None):
        if path is None:
            self._is_temporary = True
            path = _make_temporary_file('hither_temporary_file_')
        else:
            self._is_temporary = False
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