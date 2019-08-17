import htmlmin
import threading
import queue
import time

from bs4 import BeautifulSoup
from mongo import DB
from pymongo.errors import DuplicateKeyError, WriteConcernError

from utils import log, req_sleep, make_request


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "authority": "www.deutsche-biographie.de",
    "cookie": "JSESSIONID=8FB8AAE22AEC73453254170C20C8F2F2; cb-enabled=enabled; _pk_id.34.346f=b2e36e38b4f15c13.1564262154.1.1564262154.1564262154.; _pk_ses.34.346f=1",
}

default_encoding = "utf-8"

BUF_SIZE = 1000

q = queue.Queue(BUF_SIZE)

class IDCrawler(threading.Thread):
    def __init__(self, count, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super().__init__()

        self.url = "https://www.deutsche-biographie.de/search?_csrf=555737a2-d98d-406c-a5b7-eb6bde352980&name=&freitext=&gdr=&konf=&beruf=&bk=&geburtsjahr=1000-2000&todesjahr=&ortArt=geb&ort=&belOrt=&ai=&autor=&gnd=&st=erw&facets=&cf=&number={}&ot=&sl=%5B%5D&sort="
        self.count = count
        self.name = name
        self._start = 24811        

        db = DB("biblio")
        self.collection = db.get_collection("records")

        if "uid" not in self.collection.index_information().keys():
            # if we're here it means we already created the unique index
            self.collection.create_index("uid", unique=True)

    def run(self):
        pages = self.count // 10

        for p in range(self._start, pages):
            log("log.log", self.url.format(p))
            response = make_request(self.url.format(p), HEADERS, verify=False)

            if not response:
                continue
            
            soup = BeautifulSoup(response.text, "html.parser")
            response.close()
            headings = soup.find_all("h4", "media-heading")

            if len(headings) < 10:
                log("errors", f"PAGE {p} has less than 10 records")

            for h in headings:
                try:
                    uid = h.a["href"].strip(".html#nbdcontent")
                except Exception:
                    log("parse_errors", "HEADING {h} doesn't have an a tag")
                    continue
                
                try:
                    self.collection.find_one_and_update(
                        {"uid": uid}, {"$set": {"page": p}}
                    )
                except WriteConcernError as e:
                    log("db_errors_ids.log", f"Could not update {uid} - {e}")

                # try:
                #     self.collection.insert_one({"uid": uid})
                # except DuplicateKeyError as e:
                #     log("db_errors.log", e)

                if self.collection.count_documents({"uid": uid, "html": {"$eq": None}}) > 0:
                    log("id_to_page.log", uid)
                    q.put(uid)

        log("thread.log", "IDCrawler STOPPED")
        

class PageCrawler(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super().__init__()
        self.name = name

        db = DB("biblio")
        self.collection = db.get_collection("records")
        
    def run(self):
        while True and "IDCrawler" in [t.name for t in threading.enumerate()]:
            if not q.empty():
                uid = q.get()
                url = f"https://www.deutsche-biographie.de/{uid}.html#indexcontent"

                log("pages_log.log", url)

                response = make_request(url, HEADERS, verify=False)

                if not response:
                    continue

                html = htmlmin.minify(response.text)
                response.close()

                try:
                    self.collection.find_one_and_update(
                        {"uid": uid}, {"$set": {"html": html}}
                    )
                except WriteConcernError as e:
                    log("db_errors_pages.log", f"Could not update {uid} - {e}")
        
        log("thread.log", "PageCraweler STOPPED")


if __name__ == "__main__":
    id_crawler = IDCrawler(493403, name="IDCrawler")
    page_crawler = PageCrawler(name="PageCrawler")

    id_crawler.start()
    time.sleep(5)
    try:
        page_crawler.start()
    except:
        log("thread.log", "PageCrawler Exception")
