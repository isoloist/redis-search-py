# -*- coding: utf-8 -*-
import unittest


class PinyinTests(unittest.TestCase):

    def Pinyin(self, *a, **kw):
        from xpinyin import Pinyin
        return Pinyin(*a, **kw)

    def setUp(self):
        self.p = self.Pinyin()

    def test_get_pinyin_with_default_splitter(self):
        self.assertEqual(self.p.get_pinyin(u'上海'), u'shang-hai')

    def test_get_pinyin_with_splitter(self):
        self.assertEqual(self.p.get_pinyin(u'上海', splitter=u''), u'shanghai')

    def test_get_pinyin_mixed_words(self):
        self.assertEqual(self.p.get_pinyin(u'Apple发布iOS7'), u'Apple-fa-bu-iOS7')

    def test_get_initials(self):
        self.assertEqual(self.p.get_initials(u'你'), u'N')
