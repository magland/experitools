import hither
import loggery
import kachery as ka

@hither.function('addem', '0.1.3')
def addem(x, y):
    print('Adding {} and {}'.format(x, y))
    return x + y

@hither.function('count_chars', '0.1.2')
@hither.input_file('fname', required=True)
def count_chars(fname):
    with open(fname, 'r') as f:
        txt = f.read()
        return len(txt)

@hither.function('append_files', '0.1.10')
@hither.input_file('path1', required=True)
@hither.input_file('path2', required=True)
@hither.output_file('path_out', required=True)
def append_files(path1, path2, path_out):
    with open(path1, 'r') as f:
        txt1 = f.read()
    with open(path2, 'r') as f:
        txt2 = f.read()
    with open(path_out, 'w') as f:
        f.write(txt1 + txt2)

def main():
    # loggery.set_config('default_readwrite')
    loggery.set_config('local')

    result = addem.execute(x=4, y=5)
    print(result.retval)

    hash_url = ka.store_text('some sample text')
    num_chars = count_chars.execute(fname=hash_url)
    print(num_chars.retval)

    r = append_files.execute(path1=ka.store_text('test1:'), path2=ka.store_text('test2'), path_out=True)
    print(ka.load_text(r.outputs.path_out))
    num_chars = count_chars.execute(fname=r.outputs.path_out)
    print(num_chars.retval)

if __name__ == '__main__':
    main()