#!/usr/bin/python3
# -*- coding:utf-8 -*-

import _thread
import redis
from time import ctime,sleep

def blpop(conn):
    print('start:'+ctime())
    bl = conn.blpop('blist',3)
    print('end:'+ctime())
    print(bl)

def brpoplpush(conn):
    print('start:'+ctime())
    print(conn.lrange('blist1',0,-1))
    print(conn.lrange('blist2',0,-1))
    bl = conn.brpoplpush('blist1','blist2',3)
    print('end:'+ctime())
    print(bl)
    print(conn.lrange('blist1',0,-1))
    print(conn.lrange('blist2',0,-1))

if __name__ == '__main__':
    conn = redis.Redis()
    print('blpop')
    _thread.start_new_thread(blpop,(conn,))
    sleep(2)
    conn.lpush('blist',2)
    sleep(3)
    conn.delete('blist')

    print('brpoplpush')
    _thread.start_new_thread(brpoplpush,(conn,))
    sleep(2)
    conn.lpush('blist1',2)
    sleep(3)
    conn.delete('blist1','blist2')
