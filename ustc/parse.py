from bs4 import BeautifulSoup
import re
# import pprint

from pymongo.errors import WriteConcernError

from mongo import DB
from utils import log

class Parser:
    def __init__(self, classification):
        db = DB('ustc')
        self.classification = classification.replace('+', '_') if classification else 'unclassified'
        self.collection = db.get_collection(self.classification)

    def parse(self, text):
        soup = BeautifulSoup(text, features="html5lib")
        top = soup.find('div', class_='col-md-7')
        record = {}
        for p in top.find_all('p'):
            if p.label:
                label = p.text.strip(' \n\t')
                # print('label:', repr(label))
                reg = re.search(r'^([^\:]+)(:)(.*)', label)
                reg = list(map(lambda g: g.strip(' \n'), reg.groups()))
                label = reg[0].replace(' ', '_')
                content = reg[2]
                # print('label:', label)
                # print('next:', p.find_next_sibling().name)
                # print('content:', content)
                hrefs = self.get_hrefs(p)
                # print('hrefs:', hrefs)
                # print('hrefs:', repr(hrefs))
                tds = []
                # print('next:', p.find_next_sibling().name)
                if p.find_next_sibling().name == 'table':
                    tds = self.parse_tables(p.find_next_sibling())

                # print('tds:', tds)
                # print('*'*20)
                record[label] = []
                if content:
                    record[label].append(content)
                if hrefs:
                    record[label].append(hrefs)
                if tds:
                    record[label].extend(tds)

        return record

    def parse_tables(self, tag):
        rows = []
        for tr in tag.find_all('tr'):
            tds = []
            for td in tr.find_all('td'):
                if td.a:
                    tds.extend(self.get_hrefs(td))
                else:
                    tds.append(td.string)
            rows.append(tds)
        return rows

    def get_hrefs(self, tag, recursive=False):
        atags = tag.find_all('a', recursive=recursive)
        return [(a['href'], a.string) for a in atags]

    def parse_collection(self, collection):
        print('parse')
        records = collection.find({'html': {'$exists': True}})
        for r in records:
            log('{}.log'.format(self.collection.name), r['url'])
            try:
                parsed = self.parse(r['html'])
                self.collection.find_one_and_update({'url': r['url']}, {'$set': parsed})
            except WriteConcernError as e:
                    self.log('./errors_single.log', 'Could not update {} - {}'.format(r['url'], e))

    def parse_all_collections(self):
        pass

if __name__ == '__main__':
    p = Parser('Book_Trade')
    p.parse_collection(p.collection)
