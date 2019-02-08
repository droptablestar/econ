import os
import codecs
import re

def quote():
    for f in filter(lambda x: '.csv' in x, os.listdir('.')):
        print(f)
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

def get_url(c, m):
    return c[m.start():m.end()].replace(' ', '%20')

quote()
