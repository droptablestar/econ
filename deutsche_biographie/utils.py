from time import localtime, strftime
from random import uniform
from time import sleep


def log(log_name, msg):
    with open("logs/{}".format(log_name), "a") as f:
        f.write("{} {}\n".format(strftime("%Y-%d-%m_%H:%M:%S", localtime()), msg))


def req_sleep():
    timer = uniform(1.5, 2.5)
    sleep(timer)
