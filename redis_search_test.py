#!/usr/bin/env python
# -*- coding:utf-8 -*-
import redis
import json
from index import Index
from search import Search
import unittest
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

#redis config
# redis config
REDIS = {'host':'localhost','port':6379,'db':0}

class RedisSearchTestCase(unittest.TestCase):

    redis = redis.StrictRedis(host=REDIS['host'], port=REDIS['port'], db=REDIS['db'])

    @classmethod
    def setUpClass(cls):
        params = {'id':1,'title':"中国网球公开赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':2,'title':"美国网球公开赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':3,'title':"温布尔登网球公开赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':4,'title':"澳大利亚网球公开赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':5,'title':"法国网球公开赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':6,'title':"德国世界杯",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':7,'title':"全英羽毛球锦标赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)
        params = {'id':8,'title':"田径黄金联赛",'type':'Race'}
        Index(params).update(RedisSearchTestCase.redis)

    def setUp(self):
        self.redis = redis.StrictRedis(host=REDIS['host'], port=REDIS['port'], db=REDIS['db'])

    @classmethod
    def tearDownClass(cls):
        r_conn = redis.StrictRedis(host=REDIS['host'], port=REDIS['port'], db=REDIS['db']) 
        r_conn.flushdb()        


    def test_complete_redis(self):
        result = Search().complete(domain="Race",w="中国")
        print "complete result:\n"
        for x in result:
            if isinstance(x,str):
                obj = json.loads(x)
                print obj['id'],obj['title'],"\n"

    def test_complete_pinyin_redis(self):
        result = Search().complete(domain="Race",w="faguo")
        print "complete pinyin result:\n"
        for x in result:
            if isinstance(x,str):
                obj = json.loads(x)
                print obj['id'],obj['title'],"\n"

    def test_query_redis(self):
        result = Search().query(domain="Race",text='赛')
        print "query result:\n"
        for x in result:
            if isinstance(x,str):
                obj = json.loads(x)
                print obj['id'],obj['title'],"\n"


if __name__ == "__main__":
    unittest.main()

