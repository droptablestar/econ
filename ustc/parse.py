from bs4 import BeautifulSoup
import re

class Parser:
    def __init__(self, filename):
        self.filename = filename

    def parse(self):
        with open(self.filename) as fd:
            soup = BeautifulSoup(fd)
        top = soup.find('div', class_='col-md-7')
        for p in top.find_all('p'):
            if p.label:
                label = p.text.strip(' \n')
                r = re.search(r'^([^\:]+)(:)(.*)', label)
                r = list(map(lambda g: g.strip(' \n'), r.groups()))
                label = r[0]
                content = r[2]
                print('label:', label)
                print('content:', content)
                hrefs = self.get_hrefs(p)
                print('hrefs:', hrefs)
                tds = self.parse_tables(p)
                print('tds:', tds)
                print('*'*20)

    def parse_tables(self, tag):
        tds = []
        for table in tag.find_all('table'):
            print('td:', [td for td in table.find_all('td')])
            tds.extend([td.string for td in table.find_all('td')])
        return tds

    def get_hrefs(self, tag):
        atags = tag.find_all('a')
        return [a['href'] for a in atags]

if __name__ == '__main__':
    p = Parser('test1.html')
    p.parse()
