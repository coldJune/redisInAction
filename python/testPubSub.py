#！/usr/bin/python3
# -*- coding:utf-8 -*-

import redis
import _thread
import time

conn = redis.Redis()

def publisher(item):
    for i in range(item):
        conn.publish('channel', i)
        time.sleep(1)

def sub():
    _thread.start_new_thread(publisher,(3,))
    pubsub = conn.pubsub()
    pubsub.subscribe('channel')
    count = 0
    for item in pubsub.listen():
        print(item)
        count+=1
        if count == 4:
            pubsub.unsubscribe()


if __name__ == '__main__':
    sub()