from bs4 import BeautifulSoup
import re
import pprint
import htmlmin

from pymongo.errors import WriteConcernError

from mongo import DB
from utils import log

class Parser:
    def __init__(self, classification=''):
        self.db = DB('ustc')
        self.classification = classification.replace('+', '_') if classification else 'unclassified'
        self.collection = self.db.get_collection(self.classification)

    def parse(self, text):
        soup = BeautifulSoup(text, features="html5lib")
        top = soup.find('div', class_='col-md-7')
        record = {}
        first = True
        for p in top.find_all('p'):
            if p.label:
                label = p.text.strip(' \n\t')
                reg = re.search(r'^([^\:]+)(:)(.*)', label)
                reg = list(map(lambda g: g.strip(' \n'), reg.groups()))
                label = reg[0].replace(' ', '_')
                # TODO: REMOVE THIS CONDITIONAL!!
                if label != 'DIGITAL_COPY_AVAILABLE_VIA':
                    continue
                elif first:
                    record[label] = []
                    first = False
                content = reg[2]
                hrefs = self.get_hrefs(p)
                nxt = p.find_next_sibling()
                tds = self.parse_tables(nxt) if nxt and nxt.name == 'table' else []
                record[label] = record.get(label, [])
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
        return [(a.get('href'), a.string) for a in atags]

    def parse_collection(self, collection):
        pprint.pprint(collection.name)
        # records = collection.find({'html': {'$exists': True}})
        records = collection.find({'html': {'$exists': True}, 'DIGITAL_COPY_AVAILABLE_VIA': {'$exists': True}})
        for r in records:
            log('{}.log'.format(collection.name), r['url'])
            try:
                parsed = self.parse(r['html'])
                collection.find_one_and_update({'url': r['url']}, {'$set': parsed})
            except WriteConcernError as e:
                self.log('./errors_single.log', 'Could not update {} - {}'.format(r['url'], e))

    def parse_collections(self):
        for c in self.db.get_collections():
            self.parse_collection(self.db.get_collection(c))

    def minify_html(self):
        collections = self.db.get_collections()
        m = htmlmin.Minifier(remove_comments=True,remove_empty_space=True,\
                             reduce_boolean_attributes=True)
        for c in collections:
            collection = self.db.get_collection(c)
            log('mini.log', c)
            for r in collection.find({}):
                html = r.get('html', '')
                minified = m.minify(html)
                collection.find_one_and_update({'url': r['url']}, {'$set': {'html': minified}})

if __name__ == '__main__':
    p = Parser('Book_Trade')
    # p.parse_collection(p.collection)
    collections = p.db.get_collections()
    p.parse_collections()
