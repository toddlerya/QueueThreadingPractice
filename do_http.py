#!/usr/bin/env python
# coding: utf-8
# @Time     : 2018/12/6 20:56
# @Author   : toddlerya
# @FileName : do_http.py
# @Project  : QueueThreadingPractice

from __future__ import unicode_literals

import random
import requests
import Queue
import threading
from database import DataBase
import time


global task_queue_1, data_queue

task_queue_1 = Queue.Queue()
data_queue = Queue.Queue()


class FasterThread(threading.Thread):
    def __init__(self, func, *args, **kwargs):
        super(FasterThread, self).__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        print 'start', self.func.__name__, threading.current_thread().getName()
        self.func(*self.args, **self.kwargs)
        print 'finish', self.func.__name__, threading.current_thread().getName()



def get_json_data(number, *args):
    base_url = 'http://127.0.0.1:8001/api/v1/fakerfactory'
    payload = {
        'number': number,
        'columns': ','.join(*args)
    }
    resp = requests.get(base_url, params=payload)
    json_data = resp.json()
    for each in json_data['data']['records']:
        for k in args[0]:
            value = each[k]
            if k in ['name', 'age', 'email']:
                data_queue.put([k, value])
            else:
                print 'error column!', k


def download(col_list):
    while True:
        if not task_queue_1.empty():
            task = task_queue_1.get()
            get_json_data(task, col_list)
            task_queue_1.task_done()
        else:
            break


def load2db(commit_count=100):
    db = DataBase()
    if data_queue.empty():
        time.sleep(0.1)
    count = 0
    while True:
        if not data_queue.empty():
            data = data_queue.get()
            k = data[0]
            if k == 'age':
                db.load_data_to_age_tb([data[1]])
                count +=1
                data_queue.task_done()
            if k == 'name':
                db.load_data_to_name_tb([data[1]])
                count += 1
                data_queue.task_done()
            if k == 'email':
                db.load_data_to_email_tb([data[1]])
                count += 1
                data_queue.task_done()
            if count > 0 and count % commit_count == 0:
                db.conn.commit()
        else:
            db.conn.commit()
            print 'total {} line'.format(count)
            break


if __name__ == '__main__':
    print time.ctime()
    start = time.time()
    download_threads = []
    for _ in range(20):
        # number = random.randint(1, 10)
        task_queue_1.put(1000)

    for i in range(15):
        my_thread = FasterThread(download, ['name', 'age', 'email'])
        my_thread.start()
        download_threads.append(my_thread)

    load_thread = FasterThread(load2db, 10000)
    load_thread.start()

    for thread in download_threads:
        thread.join()

    load_thread.join()

    task_queue_1.join()

    end = time.time()

    print 'use {} second.'.format(end - start)
    print time.ctime()