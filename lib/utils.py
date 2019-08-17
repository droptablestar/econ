import requests

from time import localtime, strftime
from random import uniform
from time import sleep


def log(log_name, msg):
    with open("logs/{}".format(log_name), "a") as f:
        f.write("{} {}\n".format(strftime("%Y-%d-%m_%H:%M:%S", localtime()), msg))


def req_sleep():
    timer = uniform(1.5, 2.5)
    sleep(timer)


def make_request(url, headers, verify=True):
    tries = 1
    while tries > 0:
        response = requests.get(url, headers=headers, verify=verify)
        if response:
            req_sleep()
            break
        else:
            tries -= 1
            req_sleep()
    if tries != 0:
        return response
    else:
        log("request_errors.log", f"max retries - {url}")
        return None

def get_charset(self, soup):
    metas = soup.head.find_all("meta")
    if metas:
        for m in metas:
            if m.has_attr("charset"):
                return m["charset"]