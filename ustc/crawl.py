# Get all urls by collection name [*] !!Only 631,341 records!!
# Get all individual pages by url
# Parse individual page

from urllib import request
from urllib.error import HTTPError

from re import findall, search
from random import uniform
from time import sleep, localtime, strftime
from threading import Thread
import sys

from mongo import DB
from pymongo.errors import DuplicateKeyError

import numpy

from pathos.multiprocessing import ProcessingPool as Pool
from itertools import repeat
# from functools import partial
print('there')
class Crawler:
    def __init__(self, file_name=None, domain=None, classification=None):
        r''
        self.file_name = file_name
        self.domain = domain

        db = DB('ustc')
        self.classification = classification.replace('+', '_') if classification else 'unclassified'
        self.collection = db.get_collection(self.classification)
        if 'url_1' not in self.collection.index_information().keys():
            # if we're here it means we already created the unique index
            self.collection.create_index('url', unique=True)

        if self.classification != 'unclassified':
            self.url = 'https://ustc.ac.uk/index.php/search/cicero?tm_fulltext=&tm_field_allauthr=&tm_translator=&tm_editor=&ts_field_short_title=&tm_field_imprint=&tm_field_place=&sm_field_year=&f_sm_field_year=&t_sm_field_year=&sm_field_country=&sm_field_lang=&sm_field_format=&sm_field_digital=&sm_field_class=%22'+self.classification+'%22&tm_field_cit_name=&tm_field_cit_no=&order=&sm_field_ty=&start={}'
        else:
            self.url = 'https://ustc.ac.uk/index.php/search/cicero?tm_fulltext=&tm_field_allauthr=&tm_translator=&tm_editor=&ts_field_short_title=&tm_field_imprint=&tm_field_place=&sm_field_year=&f_sm_field_year=&t_sm_field_year=&sm_field_country=&sm_field_lang=&sm_field_format=&sm_field_digital=&sm_field_class=&tm_field_cit_name=&tm_field_cit_no=&order=&sm_field_ty=&start={}'

        # db.count_all()
        # exit()

    def crawl_all(self):
        def crawl(name, page_nos):
            for n in page_nos:
                self.log('{}_all.log'.format(name), '{}:{}:{}'.format(name, self.collection.name, self.url.format(n)))
                text = self.read_url(self.url.format(n))

                records = findall(r'/record/[0-9]+', text)
                for r in records:
                    try:
                        self.collection.insert_one({'url': '{}{}'.format(self.domain, r)})
                    except DuplicateKeyError as e:
                        self.log('./db_errors.log', e)

                self.req_sleep()
                return

        text = self.read_url(self.url.format(0))
        end = search(r'(Results found: )(\d+)', text)
        end = int(end.group(2)) + 1 if end else 0

        inc = search(r'(showing 1 to )(\d+)', text)
        inc = int(inc.group(2)) if end else 0

        thread_count = 6
        splits = numpy.array_split(numpy.array(range(0, end, inc)), thread_count)
        for (i, s) in zip(range(0, thread_count), splits):
            Thread(target=crawl, kwargs={'name': 't{}'.format(i), 'page_nos': s}).start()

    def crawl_single(self, name, page_nos):
        return
        records = self.collection.find({'html': {'$exists': False}})
        for r in records:
            self.log('{}.log'.format(name), '{}:{}:{}'.format(name, self.collection.name, r['url']))
            while True:
                try:
                    response = request.urlopen(r['url'])
                    self.collection.find_one_and_update({'url': r['url']}, {'$set': {'html': response.read().decode('utf-8')}})
                    break
                except Exception as e:
                    self.log('./errors_single.log', e)
                    self.req_sleep()
            self.req_sleep()

    def log(self, log_name, msg):
        with open(log_name, 'a') as f:
            f.write('{} {}\n'.format(strftime('%Y-%d-%m_%H:%M:%S', localtime()), msg))

    def req_sleep(self):
        timer = uniform(.5, 1.0)
        # print('sleeping for: {}s'.format(timer))
        sleep(timer)

    def read_url(self, url):
        while True:
            try:
                response = request.urlopen(url)
                break
            except HTTPError as e:
                self.log('./errors.log', e)
                self.req_sleep()
        text = response.read()
        try:
            text = text.decode('utf-8')
        except Exception:
            text = text.decode('latin1')
        return text

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

if __name__ == '__main__':
    if len(sys.argv) == 1:
        c = Crawler(domain='https://ustc.ac.uk/index.php')
        c.crawl_all()

    elif len(sys.argv) == 2 and sys.argv[1] == '-c' < 1:
        with open('classifications.txt') as f:
            lines = list(map(lambda line: line.strip(' \n'), f.readlines()))
            for i in range(0, len(lines)):  # create some threads
                c = Crawler(domain='https://ustc.ac.uk/index.php', classification=lines[i])
                Thread(target=c.crawl_single, kwargs={'name': 't{}'.format(i)}).start()
    else:
        print('usage: python crawl.py -c')
