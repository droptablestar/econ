# Get all urls by collection name [*] !!Only 631,341 records!!
# Get all individual pages by url
# Parse individual page

from urllib import request
from urllib.error import HTTPError

from re import findall, search
from random import uniform
from time import sleep, localtime, strftime
from threading import Thread
# import sys

from mongo import DB
from pymongo.errors import DuplicateKeyError

class Crawler:
    def __init__(self, file_name=None, domain=None, classification=None):
        self.file_name = file_name
        self.domain = domain

        db = DB('ustc')
        if classification:
            self.classification = classification
            # self.classification = parse.quote(classification(' ', '+'))
            self.collection = db.get_collection(classification.replace('+', '_'))
            if 'url_1' not in self.collection.index_information().keys():
                print('here')
                self.collection.create_index('url', unique=True)
        # db.count_all()
        # exit()

    def crawl_all(self):
        i = 115050
        end = i+1
        is_set = False
        if self.classification:
            url = 'https://ustc.ac.uk/index.php/search/cicero?tm_fulltext=&tm_field_allauthr=&tm_translator=&tm_editor=&ts_field_short_title=&tm_field_imprint=&tm_field_place=&sm_field_year=&f_sm_field_year=&t_sm_field_year=&sm_field_country=&sm_field_lang=&sm_field_format=&sm_field_digital=&sm_field_class=%22'+self.classification+'%22&tm_field_cit_name=&tm_field_cit_no=&order=&sm_field_ty=&start={}'
        else:
            url = 'https://ustc.ac.uk/index.php/search/cicero?tm_fulltext=&tm_field_allauthr=&tm_translator=&tm_editor=&ts_field_short_title=&tm_field_imprint=&tm_field_place=&sm_field_year=&f_sm_field_year=&t_sm_field_year=&sm_field_country=&sm_field_lang=&sm_field_format=&sm_field_digital=&sm_field_class=&tm_field_cit_name=&tm_field_cit_no=&order=&sm_field_ty=&start={}'
        while i <= end:
            print(url.format(i))
            print('i={}, end={}'.format(i, end))

            while True:
                try:
                    response = request.urlopen(url.format(i))
                    break
                except HTTPError as e:
                    print('here')
                    self.log('./errors.log', e)
                    self.req_sleep()

            text = response.read()
            try:
                text = text.decode('utf-8')
            except Exception:
                text = text.decode('latin1')

            if not is_set:
                is_set = True
                end = search(r'(Results found: )(\d+)', text)
                end = int(end.group(2)) if end else 0
                print('end:', end)

            records = findall(r'/record/[0-9]+', text)
            for r in records:
                try:
                    self.collection.insert_one({'url': '{}{}'.format(self.domain, r)})
                except DuplicateKeyError as e:
                    self.log('./db_errors.log', e)

            i += 25
            self.req_sleep()

    def crawl_single(self, name):
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

    def log(self, log_name, error):
        with open(log_name, 'a') as f:
            f.write('{} {}\n'.format(strftime('%Y-%d-%m_%H:%M:%S', localtime()), error))

    def req_sleep(self):
        timer = uniform(.5, 1.0)
        # print('sleeping for: {}s'.format(timer))
        sleep(timer)

if __name__ == '__main__':
    def parse(name, i, lines):
        start = i
        end = i + 1
        for j in range(start, end):
            c = Crawler(domain='https://ustc.ac.uk/index.php', classification=lines[j])
            c.crawl_single(name)
            # print('{}:{} - [{}]'.format(j, lines[j], i))
            # if i == 5:  # 37 classifications so special case for last one
            #     c = Crawler(domain='https://ustc.ac.uk/index.php', classification=lines[j+1])
            #     c.crawl_single(name)
                # print('{}:{} - [{}]'.format(j+1, lines[j+1], i))

    # c = Crawler('record_urls.txt', 'https://ustc.ac.uk/index.php', sys.argv[1] if len(sys.argv) > 1 else None)
    with open('classifications.txt') as f:
        lines = list(map(lambda line: line.strip(' \n'), f.readlines()))
        for i in range(0, 9):  # create some threads
            # print("t = Thread(target=parse, kwargs={{'name': t+str({}), 'i': {}, 'lines': lines}})".format(i, i+28))
            t = Thread(target=parse, kwargs={'name': 't'+str(i), 'i': i+28, 'lines': lines})
            t.start()

        # c = Crawler(domain='https://ustc.ac.uk/index.php', classification=lines[i])
        # for line in lines[0:1]:
        #     c = Crawler('record_urls.txt', 'https://ustc.ac.uk/index.php', line)
            # c.crawl_all()
            # c.crawl_single()
