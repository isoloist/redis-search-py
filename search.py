#!/usr/bin/env python
# -*- coding:utf-8 -*-
import redis
import logging
from base import *

class Search():
    log = logging.getLogger('search')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    """search api for complete and query"""
    def __init__(self,options={}):
        keys = options.keys()
        self.host = options['host'] if 'host' in keys else 'localhost'
        self.port = options['port'] if 'port' in keys else    6379
        self.db = options['db'] if 'db' in keys else 0
        self.complete_max_length = options['complete_max_length'] if 'complete_max_length' in keys else 200
        self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)

    """query by prefix"""
    def complete(self,domain=None,w=None,options={}):
        if domain == None or w == None:
            return []
        r = redis.Redis(connection_pool=self.pool)
        limit = options['limit'] if 'limit' in options.keys() else 10
        conditions = options['conditions'] if 'conditions' in options.keys() else []
        if(((w == None or w == "") and (conditions == None or len(conditions) == 0 )) or (domain == None or "" == domain)):
            return []
        # keyword matched with the w
        prefix_matches = []
        range_len = self.complete_max_length
        prefix = w.lower()
        key = mk_complete_key(domain)
        start = r.zrank(key,prefix)
        if start == None:
            start = r.zrank(key,prefix+"*")
        Search.log.debug("zrank start = %s " % start)
        if start != None :
            count = limit
            max_range = start +(range_len*limit) -1
            word_range = r.zrange(key,start,max_range)
            while len(prefix_matches) <= count:
                start += range_len
                if word_range == None or len(word_range) == 0:
                    break
                for entry in word_range:
                    min_len = min(len(entry),len(prefix))
                    if(entry[:min_len] != prefix[:min_len]):
                        count = len(prefix_matches)
                        break
                    if entry[-1:] == "*" and len(prefix_matches) != count:
                        entry = str(entry)
                        prefix_matches.append(entry[:-1])
                if start >= len(word_range):
                    word_range = []
                else:
                    word_range = word_range[start:min((max_range-1),(len(word_range)-1))]
        Search.log.debug("prefix_matches = %s " % ",".join(prefix_matches))
        words = []
        for prefix_match in {}.fromkeys(prefix_matches).keys():
            words.append(mk_sets_key(domain,prefix_match))
        conditions_keys = []
        if conditions != None and len(conditions) > 0 :
            if isinstance(conditions,list):
                conditions = conditions[0]

            if isinstance(conditions,dict):
                for key,value in conditions.items():
                    conditions_keys.append(mk_condition_key(domain,key,value))
        temp_store_key = "tmpsunionstore:"+"+".join(words)
        if len(words) > 0:
            if r.exists(temp_store_key) == False:
                r.sunionstore(temp_store_key,words)
                r.expire(temp_store_key,86400)
        else:
            if len(words) == 1:
                temp_store_key = words[0]
            else:
                return []
        if len(conditions_keys) > 0:
            if words and len(words) > 0:
                conditions_keys.append(temp_store_key)
            temp_store_key = "tmpsinterstore:"+"+".join(conditions_keys)
            if r.exists(temp_store_key) == False:
                r.sinterstore(temp_store_key,conditions_keys)
                r.expire(temp_store_key,86400)
        ids = r.sort(temp_store_key,desc=True,start=0,num=limit,by=mk_score_key(domain,"*"))
        return r.hmget(domain,ids) if len(ids) > 0 else []


    """query by keyword"""
    def query(self,domain=None,text=None,options={}):
        if domain == None or text == None:
            return []
        text = text.lower()
        r =  redis.Redis(connection_pool=self.pool)
        limit = options['limit'] if 'limit' in options.keys() else 10
        sort = options['sort'] if 'sort' in options.keys() else 'id'
        conditions = options['conditions'] if 'conditions' in options.keys() else []
        pinyin_match = options['pinyin_match'] if 'pinyin_match' in options.keys() else False

        if text.strip() == "" or (conditions and len(conditions) == 0):
            return []
        texts = split(text)
        # Search.log.debug("query texts = %s " % ",".join([t.encode('utf-8') for t in texts]))
        words = []
        for t in texts:
            words.append(mk_sets_key(domain,t))

        conditions_keys = []
        if conditions and len(conditions) > 0:
            if isinstance(conditions,list):
                conditions = conditions[0]
            if isinstance(conditions,dict):
                for key,value in conditions.items():
                    conditions_keys.append(mk_condition_key,domain,key,value)
            words.extend(conditions_keys)

        if words == None or len(words) == 0:
            return []

        temp_store_key = "tmpinterstore:"+"+".join(words)
        # Search.log.debug("query words = %s " % ",".join([word.encode('utf-8') for word in words]))
        if len(words) > 0:
            if r.exists(temp_store_key) == False:
                r.sinterstore(temp_store_key,words)
                r.expire(temp_store_key,86400)
        if pinyin_match:
            pinyin_texts = split_pinyin(text)
            pinyin_words = []
            for pinyin_text in pinyin_texts:
                pinyin_words.append(mk_sets_key(domain,pinyin_text))
            pinyin_words.extend(conditions_keys)
            temp_sunion_key = "tmpsunionstore:"+"+".join(pinyin_words)
            temp_pinyin_store_key = "tmpinterstore:"+"+".join(pinyin_words)
            r.sinterstore(temp_pinyin_store_key,pinyin_words)
            r.sunionstore(temp_sunion_key,[temp_store_key,temp_pinyin_store_key])
            r.expire(temp_pinyin_store_key,86400)
            r.expire(temp_sunion_key,86400)
            temp_store_key = temp_sunion_key

        ids = r.sort(temp_store_key,desc=True,start=0,num=limit,by=mk_score_key(domain,"*"))
        return r.hmget(domain,ids) if len(ids) > 0 else []





