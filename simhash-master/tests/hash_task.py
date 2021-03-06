# coding: utf-8

# from __future__ import division, unicode_literals

import sys,os
import re,time
import hashlib
import logging
import collections
from itertools import groupby
from boto3 import Session
from Tools.db1 import *
reload(sys)
sys.setdefaultencoding('utf-8')


# 测试settings:文件路径，注意不要加'/'
driver_path = './report/17'
# 海明距离
diatance = 3

# 14号文件夹出现不同的结果
'''抽取核心算法，python2测试文本，有些情况下算出来的哈希值，跟直接调用的值不相等！！！'''

if sys.version_info[0] >= 3:
    basestring = str
    unicode = str
    long = int
else:
    range = xrange

def _hashfunc(x):
    return int(hashlib.md5(x).hexdigest(), 16)


class Simhash(object):

    def __init__(self, value, f=64, reg=r'[\w\u4e00-\u9fcc]+', hashfunc=None):
        """
        value是basestring并描述在解析的字符串中被认为是一个字母
        `f` is the dimensions of fingerprints
        `reg` is meaningful only when `value` is basestring and describes
        what is considered to be a letter inside parsed string. Regexp
        object can also be specified (some attempt to handle any letters
        is to specify reg=re.compile(r'\w', re.UNICODE))
        `hashfunc` accepts a utf-8 encoded string and returns a unsigned
        integer in at least `f` bits.
        """

        self.f = f
        self.reg = reg
        self.value = None
        if hashfunc is None:
            self.hashfunc = _hashfunc
        else:
            self.hashfunc = hashfunc

        if isinstance(value, Simhash):
            self.value = value.value
        elif isinstance(value, basestring):
            self.build_by_text(unicode(value))
        elif isinstance(value, collections.Iterable):
            self.build_by_features(value)
        elif isinstance(value, long):
            self.value = value
        else:
            raise Exception('Bad parameter with type {}'.format(type(value)))

    def _slide(self, content, width=4):
        return [content[i:i + width] for i in range(max(len(content) - width + 1, 1))]

    def _tokenize(self, content):
        content = content.lower()# 小写
        content = ''.join(re.findall(self.reg, content))# 正则处理
        ans = self._slide(content)
        return ans

    def build_by_text(self, content):
        features = self._tokenize(content)
        features = {k:sum(1 for _ in g) for k, g in groupby(sorted(features))}
        return self.build_by_features(features)

    def build_by_features(self, features):
        """
        `features` might be a list of unweighted tokens (a weight of 1
                   will be assumed), a list of (token, weight) tuples or
                   a token -> weight dict.
        """
        v = [0] * self.f
        masks = [1 << i for i in range(self.f)]
        if isinstance(features, dict):
            features = features.items()
        for f in features:
            if isinstance(f, basestring):
                h = self.hashfunc(f.encode('utf-8'))
                w = 1
            else:
                assert isinstance(f, collections.Iterable)
                h = self.hashfunc(f[0].encode('utf-8'))
                w = f[1]
            for i in range(self.f):
                v[i] += w if h & masks[i] else -w
        ans = 0
        for i in range(self.f):
            if v[i] > 0:
                ans |= masks[i]
        self.value = ans

    def distance(self, another):
        """
        计算海明距离，海明距离在二进制中表现为 xor，数出1的个数
        """
        assert self.f == another.f
        x = (self.value ^ another.value) & ((1 << self.f) - 1)
        ans = 0
        while x:
            ans += 1
            x &= x - 1
        return ans


# 计算海明距离：有多少是相同的
def hammingDis(com1, com2):
    t1 = '0b' + com1
    t2 = '0b' + com2
    n = int(t1, 2) ^ int(t2, 2)
    i = 0
    while n:
        n &= (n - 1)
        i += 1
    return i

def news_process(file_path):
    '''读取文件'''
    news_file = open(file_path, 'r')
    news = news_file.read()
    # 判断为空文件2
    if news == '':
        with open('hash_empty.log', 'a+') as fp:
            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
            fp.write(u'{}空文件...{}'.format(file_path, now_time2) + '\n')
            fp.write('=' * 30 + '\n')
        return
    news_file.close()
    return news.decode("utf-8")

def num10_to2_sys(num):
    '''10进制转成2进制'''
    num1 = None
    if isinstance(num, str):
        num1 = bin(int(num))
    elif isinstance(num, long):
        num1 = bin(num)
    elif isinstance(num, int):
        num1 = bin(num)
    num2 = num1.replace('0b', '')
    if len(num2) < 64:
        num2 = "0"*(64 - len(num2)) + num2
    elif len(num2) > 64:
        print u'超出64位[error...]'
        return
    return num2

def num2_to10_sys(num):
    '''10进制转成2进制'''
    num1 = None
    if isinstance(num, str):
        num1 = int(num, 2)
    elif isinstance(num, long):
        num1 = int(str(num), 2)
    elif isinstance(num, int):
        num1 = int(str(num), 2)
    num2 = str(num1).replace('L', '')
    return num2

# 查询数据库添加哈希值
def sql_select():
    if not os.path.exists('hash_select.log'):
        with open('hash_select.log', 'w') as f:
            f.write('')
    f1 = open('hash_select.log', 'r')
    try:
        # 接上一次
        content = f1.readlines()[-1]
    except:
        # 全部
        content = ''
    print u'添加哈希,记录', content
    sqlstr1 = "SELECT TOP 2000 ReportOriginal, ReportCode FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE Simhash1 is NULL AND ReportCode > '{}' order by ReportCode asc".format(content)
    result = execute_SqlServer_select(sqlstr1)
    is_error = False
    for ReportOriginal, ReportCode in result:
        # 添加哈希
        path = S3(ReportOriginal)
        if path:
            cont = news_process(path)
            if cont:
                simhash = Simhash(cont)
                key = num10_to2_sys(simhash.value)
                print ReportCode, ReportOriginal
                print key
                os.remove(path)
                if key:
                    # 更新数据库
                    Simhash1 = key[:16]
                    Simhash2 = key[16:32]
                    Simhash3 = key[32:48]
                    Simhash4 = key[48:64]
                    sql_updata = "UPDATE Report_ReportBaseInfo_Xbrl_copy3 SET Simhash = '{0}', Simhash1 = '{1}', Simhash2 = '{2}', Simhash3 = '{3}', Simhash4 = '{4}'  WHERE ReportCode = '{5}'".format(
                        key, Simhash1, Simhash2, Simhash3, Simhash4, ReportCode)
                    if execute_SqlServer_updata(sql_updata):
                        with open('error_hash.log', 'a+') as fp:
                            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                            fp.write(u'生成哈希失败...{}'.format(now_time2) + '\n')
                            fp.write(sql_updata + u'更新失败...' + '\n')
                            fp.write('=' * 30 + '\n')
                        is_error = True
                        break
                else:
                    with open('error_hash.log', 'a+') as fp:
                        now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                        fp.write(u'生成哈希失败...{}'.format(now_time2) + '\n')
                        fp.write(ReportCode+ u'超出64位哈希...' + '\n')
                        fp.write('=' * 30 + '\n')
                    is_error = True
                    break
        # 记录
        with open('hash_select.log', 'w') as fp:
            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
            fp.write(u'生成哈希...' + now_time2 + '\n' + str(ReportCode))
    if is_error:
        print '哈希[error...]'
    else:
        print '生成哈希[ok]'

# 处理重复
def sql_select_simhash():
    if not os.path.exists('hash_code.log'):
        with open('hash_code.log', 'w') as f:
            f.write('')
    f1 = open('hash_code.log', 'r')
    try:
        # 接上一次
        content = f1.readlines()[-1]
    except:
        # 全部
        content = ''
    print u'去重,记录', content
    # 所有数据，排序
    sqlstr0 = "SELECT ReportCode, RepeatCode, Simhash FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE Simhash1 is NOT NULL order by ReportCode asc"
    result0 = execute_SqlServer_select(sqlstr0)

    # 当前所有没有处理的，排序
    try:
        sqlstr1 = "SELECT ReportCode, RepeatCode, Simhash FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE Simhash1 is NOT NULL AND ReportCode > '{}' order by ReportCode asc".format(content)
        result = execute_SqlServer_select(sqlstr1)
    except:
        return
    if result:
        is_error = False
        for ReportCode, RepeatCode, Simhash in result:
            # # 之前，四段法
            # index = result0.index((ReportCode, RepeatCode, Simhash))
            # print u'下标', index
            # print u'ReportCode', ReportCode
            # if index > 0:
            #     new_RepeatCode = []
            #     fw = [Simhash[:16], Simhash[16:32], Simhash[32:48], Simhash[48:64]]
            #     for ReportCode2, RepeatCode2, Simhash2 in result0[:index]:
            #         # 四段有无一样的
            #         if Simhash[:16] in fw  or Simhash[16:32] in fw  or Simhash[32:48] in fw  or Simhash[48:64] in fw :
            #             num = hammingDis(Simhash, Simhash2)
            #             # 小于海明距离的范围
            #             if num <= diatance:
            #                 print u'海明距离', num
            #                 new_RepeatCode.append(unicode(ReportCode2))
            #     if new_RepeatCode:
            #         new_RepeatCode1 = ','.join(new_RepeatCode)
            #         sql_updata = "UPDATE Report_ReportBaseInfo_Xbrl_copy3 SET RepeatCode = '{0}' WHERE ReportCode = '{1}'".format(unicode(new_RepeatCode1), ReportCode)
            #         if execute_SqlServer_updata(sql_updata):
            #             with open('error_hash.log', 'a+') as fp:
            #                 now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
            #                 fp.write(u'去重失败...{}'.format(now_time2) + '\n')
            #                 fp.write(sql_updata + u'更新失败...' + '\n')
            #                 fp.write('=' * 30 + '\n')
            #             is_error = True
            #             break

            # 之前
            index = result0.index((ReportCode, RepeatCode, Simhash))
            # print u'下标', index
            print u'ReportCode', ReportCode
            if index > 0:
                new_RepeatCode = []
                for ReportCode2, RepeatCode2, Simhash2 in result0[:index]:
                    num = hammingDis(Simhash, Simhash2)
                    # 小于海明距离的范围
                    if num <= diatance:
                        print u'海明距离', num
                        new_RepeatCode.append(unicode(ReportCode2))
                if new_RepeatCode:
                    new_RepeatCode1 = ','.join(new_RepeatCode)
                    sql_updata = "UPDATE Report_ReportBaseInfo_Xbrl_copy3 SET RepeatCode = '{0}' WHERE ReportCode = '{1}'".format(unicode(new_RepeatCode1), ReportCode)
                    if execute_SqlServer_updata(sql_updata):
                        with open('error_hash.log', 'a+') as fp:
                            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                            fp.write(u'去重失败...{}'.format(now_time2) + '\n')
                            fp.write(sql_updata + u'更新失败...' + '\n')
                            fp.write('=' * 30 + '\n')
                        is_error = True
                        break
            # 记录
            with open('hash_code.log', 'w') as fp:
                now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                fp.write(u'去重...' + now_time2 + '\n' + str(ReportCode))
        if is_error:
            print '去重[error...]'
        else:
            print "去重[ok]"

# S3用法--- 下载文件txt
def S3(path):
    # Client初始化
    access_key = 'AKIAOFQU43PDALLGLKSQ'
    secret_key = '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'
    session = Session(access_key, secret_key, region_name='cn-north-1')
    s3_client = session.client('s3')

    path1 = re.search(r'/(re.+)\.(.+)', path).group(1)
    print path1
    datapool_path = path1 + '.txt'
    try:
        resp = s3_client.get_object(Bucket="datapool", Key=datapool_path)

        down_path = './' + re.search(r'(re.+?/2017/\d+/\d.+?)\d+', path1).group(1)
        down_file = './' + datapool_path
        print down_file
        if not os.path.exists(down_path):
            os.makedirs(down_path)
        if not os.path.exists(down_file):
            with open(down_file, 'wb') as f:
                f.write(resp['Body'].read())
            return down_file
        else:
            return down_file
    except Exception as e:
        return


def test():
    # po = Pool(10)
    for dirpath, dirnames, filenames in os.walk(driver_path):
        for filename in filenames:
            index = filenames.index(filename)
            print '下标', index
            file_path1 = dirpath + '/' + filename
            cont = news_process(file_path1)
            simhash1 = Simhash(cont)
            print file_path1
            key1 = num10_to2_sys(simhash1.value)
            print key1
            for i in filenames[:index]:
                file_path2 = dirpath + '/' + i
                cont2 = news_process(file_path2)
                simhash2 = Simhash(cont2)
                key2 = num10_to2_sys(simhash2.value)
                a = hammingDis(key1, key2)
                print '海明距离', a
                print file_path1
                print key1
                print file_path2
                print key2


if __name__ == '__main__':
    # sql_select()
    # sql_select_simhash()
    test()


    # # 这3个算出来的哈希值一样，可是内容不一样！！===>>>修改核心算法，减掉排序sorted，效果上去了
    # with open(r'./report/17/300005413903_20170802_EA4209.txt', 'r', encoding='utf8') as f:
    #     content1 = f.read()
    #     # print content1
    # with open(r'./report/17/300005413399_20170802_D1F391.txt', 'r', encoding='utf8') as f2:
    #     content2 = f2.read()
    #     # print content2
    # with open(r'./report/17/300005409773_20170801_5A4B55.txt', 'r', encoding='utf8') as f3:
    #     content3 = f3.read()
    #     # print content3
    #
    # print (type(content1))
    # # simhash1 = Simhash(content1.decode("utf8"))
    # # # simhash1 = Simhash('1'.decode("utf8"))
    # # simhash2 = Simhash(content2.decode("utf8"))
    # # simhash3 = Simhash(content3.decode("utf8"))
    # simhash1 = Simhash(content1)
    # # simhash1 = Simhash('1'.decode("utf8"))
    # simhash2 = Simhash(content2)
    # simhash3 = Simhash(content3)
    #
    # print (simhash1.value, "-----", simhash2.value, '====', simhash3.value)
    # print (simhash1.value)
    # print ('海明', simhash1.distance(simhash2))


