import os
import codecs
import re

for f in filter(lambda x: '.csv' in x, os.listdir('.')):
    print(f)
    with codecs.open(f, encoding='utf8') as fd:
        c = fd.read()
        c = c.encode('utf8').decode('unicode_escape')
        for m in re.finditer('\"https?://[^\"]+\"', c):
            c = c.replace(m.group(), m.group().replace(' ', '%20'))
    with codecs.open(f, 'w', encoding='utf8') as fd:
        fd.write(c)
