#!/usr/bin/env python
# coding: utf-8
# @Time     : 2018/12/6 20:34
# @Author   : toddlerya
# @FileName : database.py
# @Project  : QueueThreadingPractice

from __future__ import unicode_literals

import sqlite3


class DataBase(object):
    def __init__(self):
        # self.conn = sqlite3.connect('spdb.db')
        self.conn = sqlite3.connect('spdb.db', check_same_thread = False)
        self.cur = self.conn.cursor()

    def load_data_to_name_tb(self, *args):
        insert_sql = '''
        insert into name_tb (user_name) values (?)
        '''
        self.cur.execute(insert_sql, *args)

    def load_data_to_age_tb(self, *args):
        insert_sql = '''
        insert into age_tb (age) values (?)
        '''
        self.cur.execute(insert_sql, *args)

    def load_data_to_email_tb(self, *args):
        insert_sql = '''
        insert into email_tb (email) values (?)
        '''
        self.cur.execute(insert_sql, *args)


if __name__ == '__main__':
    db = DataBase()
    db.load_data_to_age_tb(['222'])
    db.conn.commit()