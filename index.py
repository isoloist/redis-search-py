#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Index
add index and remove index from redis
"""
import redis
import json
import sys
import logging
from base import *

reload(sys)
sys.setdefaultencoding('utf-8')

class Index:
    log = logging.getLogger('index')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    """save an instance to redis index"""
    def __init__(self,options):
        self.prefix_index_enable = True
        self.aliases = []
        self.exts = {}
        self.conditions = []
        for key,value in options.items():
            self.__dict__[key] = value
        self.aliases.append(self.title.lower())
        self.aliases = {}.fromkeys(self.aliases).keys()

    def save(self,redis=None):
        if redis == None:
            return
        if self.title == None or self.title.strip() == "":
            return
        data  = {'title':self.title,'id':self.id}
        for key,value in self.exts.items():
            data[key] = value
        pipeline = redis.pipeline()
        """set instance to hashset """
        pipeline.hset(self.type,self.id,json.dumps(data))

        """save condition fields in sorted set"""
        for field in self.conditions:
            pipeline.sadd(mk_condition_key(self.type,field,data[field]),self.id)

        """save aliases"""
        for alias in self.aliases:
            words = Index.split_words_for_index(alias)
            if len(words) == 0:
                return
            for word in words:
                pipeline.sadd(mk_sets_key(self.type,word),self.id)
        pipeline.execute()


        """save prefix"""
        if self.prefix_index_enable:
            self.save_prefix_index(redis)

    # update record.if the title has no change, it won't update the index
    def update(self,redis=None,pipeline=None):
        record = redis.hget(self.type,self.id)
        if record != None:
            record = json.loads(record)
            if record['title'] == self.title:
                return
            else:
                Index.remove(redis=redis,options={'id':self.id,'type':self.type,'title':record['title']})
        self.save(redis=redis)



    def save_prefix_index(self,redis=None,pinyin_match=True):
        key = mk_complete_key(self.type)
        pipeline  = redis.pipeline()
        for alias in self.aliases:
            words = []
            words.append(alias)
            pipeline.sadd(mk_sets_key(self.type,alias),self.id)
            if pinyin_match:
                pinyin_full = split_pinyin(alias)
                pinyin_first = []
                for py in pinyin_full:
                    if len(py) > 0:
                        pinyin_first.append(py[0])
                pinyinStr = "".join(pinyin_full)
                words.append(pinyinStr)
                words.append("".join(pinyin_first))
                pipeline.sadd(mk_sets_key(self.type,pinyinStr),self.id)
                pinyin_full = None
                pinyin_first = None
                pinyinStr = None
            for word in words:
                for xlen in range(1,len(word)):
                    prefix = word[0:xlen]
                    pipeline.zadd(key,0,prefix)
                pipeline.zadd(key,0,word+"*")
            pipeline.execute()
    @staticmethod
    def split_words_for_index(title,pinyin_match=True):
        words = split(title)
        if pinyin_match:
            pinyin_full = split_pinyin(title)
            pinyin_first = []
            for py in pinyin_full:
                if len(py) > 0 :
                    pinyin_first.append(py[0])
            words += pinyin_full
            words.append("".join(pinyin_first))
            pinyin_full = None
            pinyin_first = None
        return {}.fromkeys(words).keys()

    # remove an instance from redis index. ex options={'type':'Contact','id':2310,'title':u'黄旻娇'}
    @staticmethod
    def remove(redis=None,options={}):
        if redis == None or 'type' not in options.keys() or \
                        'id' not in options.keys():
            return
        instance_type = options['type']
        instance_id = options['id']
        instance = redis.hget(instance_type,instance_id)
        if instance == None:
            return
        instance_title = json.loads(instance)['title']
        redis.hdel(instance_type,instance_id)
        words = Index.split_words_for_index(instance_title)
        pipeline = redis.pipeline()
        for word in words:
            pipeline.srem(mk_sets_key(instance_type,word),instance_id)
            pipeline.delete(mk_score_key(instance_type,instance_id))
        pipeline.srem(mk_sets_key(instance_type,instance_title),instance_id)
        pipeline.execute()

