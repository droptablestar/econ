import requests
from bs4 import BeautifulSoup
from mongo import DB
from pymongo.errors import DuplicateKeyError

from utils import log, req_sleep


INDEX_URL = "https://www.deutsche-biographie.de/{}.html#indexcontent"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "authority": "www.deutsche-biographie.de",
    "cookie": "JSESSIONID=8FB8AAE22AEC73453254170C20C8F2F2; cb-enabled=enabled; _pk_id.34.346f=b2e36e38b4f15c13.1564262154.1.1564262154.1564262154.; _pk_ses.34.346f=1",
}

default_encoding = "utf-8"


class Crawler:
    def __init__(self, start, count, url):
        self.count = count
        self.url = url
        self.start = start
        db = DB("biblio")

        self.collection = db.get_collection("records")
        if "uid" not in self.collection.index_information().keys():
            # if we're here it means we already created the unique index
            self.collection.create_index("uid", unique=True)

    def crawl(self):
        pages = self.count // 10

        for p in range(self.start, pages+1):
            log("log.log", url.format(p))
            tries = 10
            while tries > 0:
                response = requests.get(url.format(p), headers=HEADERS, verify=False)
                if response:
                    req_sleep()
                    break
                else:
                    tries -= 1
                    req_sleep()
            if tries == 0:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            headings = soup.find_all("h4", "media-heading")

            if len(headings) < 10:
                log("errors", f"PAGE {p} has less than 10 records")

            for h in headings:
                try: 
                    uid = h.a["href"].strip(".html#nbdcontent")
                except Exception:
                    log("errors", "HEADING {h} doesn't have an a tag")
                    continue
                try:
                    self.collection.insert_one({"uid": uid})
                except DuplicateKeyError as e:
                    log("db_errors.log", e)

    def get_charset(self, soup):
        metas = soup.head.find_all("meta")
        if metas:
            for m in metas:
                if m.has_attr("charset"):
                    return m["charset"]


if __name__ == "__main__":
    log("log.log", f"************* --------- :RESET: **************** ------- 1720")
    url = "https://www.deutsche-biographie.de/search?_csrf=555737a2-d98d-406c-a5b7-eb6bde352980&name=&freitext=&gdr=&konf=&beruf=&bk=&geburtsjahr=1000-2000&todesjahr=&ortArt=geb&ort=&belOrt=&ai=&autor=&gnd=&st=erw&facets=&cf=&number={}&ot=&sl=%5B%5D&sort="
    crawler = Crawler(49339, 493403, url)
    crawler.crawl()