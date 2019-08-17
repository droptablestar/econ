import os
import codecs
import re

def quote():
    for f in filter(lambda x: '.csv' in x, os.listdir('.')):
        print(f)
        try:
            with codecs.open(f, encoding='utf8') as fd:
                c = fd.read().encode('utf8').decode('unicode_escape')
                i = re.finditer('\"https?://[^\"]+\"', c)
                prev = next(i)
                new_str = c[:prev.start()] + get_url(c, prev)
                for cur in i:
                    new_str += c[prev.end():cur.start()] + get_url(c, cur)
                    prev = cur
            with codecs.open(f, 'w', encoding='utf8') as fd:
                fd.write(new_str)
        except Exception as e:
            print(f)
            print(e)

def get_url(c, m):
    return c[m.start():m.end()].replace(' ', '%20')

def encoding():
    for fn in os.listdir('.'): 
        if '2' not in fn and 'failed' not in fn and 'decode' not in fn:
            try: 
                with codecs.open(fn, encoding='utf-8') as fd:
                    text = fd.read()
                    text = text.encode('Windows-1252', errors='ignore').decode('utf-8', errors='ignore')
                with codecs.open(fn[:fn.rfind('.')]+'2.csv', 'w', encoding='utf-8') as fd: 
                        fd.write(text)
            except Exception as ex: 
                print('*'*50, '\n')
                print(fd.name)
                print(ex)
                print('*'*50, '\n')
    
encoding()
