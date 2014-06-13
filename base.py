#!/usr/bin/env python
# -*- coding:utf-8 -*-
from xpinyin import Pinyin


pinyin = Pinyin()

def mk_sets_key(instance_type,key):
    return "%s:%s" % (instance_type,key)

def mk_score_key(instance_type,instance_id):
    return "%s:_score_:%s" % (instance_type,instance_id)

def mk_condition_key(instance_type,field,value):
    return "%s:_by:_%s:%s" % (instance_type,field,value)

def mk_complete_key(instance_type):
    return "Compl%s" % instance_type

def split_pinyin(text):
    if not isinstance(text,unicode):
        text = text.decode('utf-8')
    return pinyin.get_pinyin(text).split("-")


def split(text):
    return split_ex(text)

def split_ex(text,disable_seg=True):
    if disable_seg:
        # split the text into a list word by word
        return list(text)