import hither

@hither.function('addem', '0.1.3')
@hither.container(default='docker://python:3.7')
def addem(x, y):
    print('Adding {} and {}'.format(x, y))
    return x + y

@hither.function('count_chars', '0.1.2')
@hither.input_file('fname', required=True)
@hither.container(default='docker://python:3.7')
def count_chars(fname):
    with open(fname, 'r') as f:
        txt = f.read()
        return len(txt)

@hither.function('append_files', '0.1.11')
@hither.input_file('path1', required=True)
@hither.input_file('path2', required=True)
@hither.output_file('path_out', required=True)
@hither.container(default='docker://python:3.7')
def append_files(path1, path2, path_out):
    with open(path1, 'r') as f:
        txt1 = f.read()
    with open(path2, 'r') as f:
        txt2 = f.read()
    with open(path_out, 'w') as f:
        f.write(txt1 + txt2)

def main():
    import loggery
    import kachery as ka

    # loggery.set_config('default_readwrite')
    loggery.set_config('local')
    container = 'default'
    # container = None
    force_run = False

    result = addem.execute(x=4, y=5, _container=container, _force_run=force_run)
    print(result.retval)

    hash_url = ka.store_text('some sample text')
    num_chars = count_chars.execute(fname=hash_url, _container=container, _force_run=force_run)
    print(num_chars.retval)

    r = append_files.execute(path1=ka.store_text('test1:'), path2=ka.store_text('test2'), path_out=hither.File(), _container=container, _force_run=force_run)
    print(ka.load_text(r.outputs.path_out._path))
    num_chars = count_chars.execute(fname=r.outputs.path_out, _container=container, _force_run=force_run)
    print(num_chars.retval)

if __name__ == '__main__':
    main()