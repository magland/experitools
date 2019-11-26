import loggery

def main():
    loggery.set_config('default_readwrite')
    
    loggery.insert_one({'test1': 1, 'test2': 4})
    doc = loggery.find_one(query={'message.test1': 1})
    print(doc)

if __name__ == '__main__':
    main()