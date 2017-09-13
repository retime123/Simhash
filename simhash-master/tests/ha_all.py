# coding: utf-8

import jieba
import jieba.posseg as pseg
import numpy as np
import pandas as pd
import re,json
import os

import time
from Tools.db1 import *
from boto3 import Session
# from tools import proc_text

import threading
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Manager,Pool
# from nltk.corpus import stopwords
import nltk


# now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
# settings:文件路径，注意不要加'/'
driver_path = './data/dir_copy'
# driver_path = './data/news'
# 海明距离
diatance = 2


# 加载停用词
stopwords1 = [line.rstrip() for line in open('./CN_stopwords.txt', 'r')]

stopwords = stopwords1
# print stopwords


# 哈希算法
class SimHash(object):
    def news_process(self, file_path):
        '''去除空格'''
        # 判断为空文件1
        # size = os.path.getsize(file_path)
        # if size == 0:
        #     with open('hash_empty.log', 'ab+') as fp:
        #         fp.write('{}空文件...{}'.format(file_path, now_time2) + '\n')
        #         fp.write('=*' * 30 + '\n')
        #     return
        news_file = open(file_path, 'r')
        news = news_file.read()
        # 判断为空文件2
        if news == "":
            with open('hash_empty.log', 'a+') as fp:
                now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                fp.write('{}空文件...{}'.format(file_path, now_time2) + '\n')
                fp.write('=*' * 30 + '\n')
            return
        # print type(news)
        news = re.sub(r'\s*', '', news)# 等价[\n\r\t\f]
        # news = re.sub(r'[\w]+', '', news)
        news = re.sub(u'[\[\]@\.！。!：:；;,，\-_—/（()）％·…]*', '', news)
        # news = news.replace('\t', '').replace('\n', '').replace(' ', '')
        news = re.sub(r'</?.+?>', '', news)
        # news = news.replace('。','').replace('%','').replace('', '')
        news_file.close()
        print news
        return news

    def proc_text(self, raw_line):
        """
            处理每行的文本数据
            返回分词结果
        """
        # 1. 使用正则表达式去除非中文字符
        # filter_pattern = re.compile('[^\u4E00-\u9FD5]+')
        filter_pattern = re.compile('\s*')
        # 将所有非中文字符替换为""
        chinese_only = filter_pattern.sub('', raw_line)

        # 2. 结巴分词+词性标注
        # 返回分词结果列表，包含单词和词性
        # words_lst = pseg.cut(chinese_only)
        words_lst = jieba.cut(chinese_only)

        # 3. 去除停用词
        # 将所有非停用词的词语存到列表里
        meaninful_words = []
        for word in words_lst:
            # if (word not in stopwords) and (flag == 'v'):
            # 也可根据词性去除非动词等
            if word not in stopwords:
                meaninful_words.append(word)

        print meaninful_words
        # 返回一个字符串
        # return ''.join(meaninful_words)
        return meaninful_words


    def string_hash(self, source):
        if source == "":
            source = '无内容'
        x = ord(source[0]) << 7
        m = 1000003
        mask = 2 ** 128 - 1
        for c in source:
            x = ((x * m) ^ ord(c)) & mask
        x ^= len(source)
        if x == -1:
            x = -2
        x = bin(x).replace('0b', '').zfill(64)[-64:]
        return str(x)

    def get_simhash(self, file_path):
        '''生成哈希值'''
        news = self.news_process(file_path)
        # news = self.proc_text(news)
        # if news is None:
        #     return
        # # cut_all=True 全模式;   cut_all=False 精确模式(默认)
        words = jieba.cut(news, cut_all=False)
        # 搜索引擎模式
        # words = jieba.cut_for_search(news)
        # words = news
        try:
            hashList = []
            for w in words:
                hash_o = np.array(list(map(int, self.string_hash(w)))) * 2 - 1
                hashList.append(hash_o)
            simhash_list = list(np.sum(np.array(hashList), axis=0))
        except:
            with open('error_hash.log', 'a+') as fp:
                now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                fp.write('生成哈希失败...{}'.format(now_time2) + '\n')
                fp.write(file_path + '文件异常...' + '\n')
                fp.write('=' * 30 + '\n')
            return
        simhash = ""
        for h in simhash_list:
            if h > 0:
                h = '1'
            else:
                h = '0'
            simhash = simhash + h
        return simhash

    # 计算海明距离：有多少是相同的
    def hammingDis(self, com1, com2):
        t1 = '0b' + com1
        t2 = '0b' + com2
        n = int(t1, 2) ^ int(t2, 2)
        i = 0
        while n:
            n &= (n - 1)
            i += 1
        return i


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
    print content
    # 所有数据
    sqlstr0 = "SELECT ReportCode, RepeatCode, Simhash FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE Simhash1 is NOT NULL order by ReportCode asc"
    result0 = execute_SqlServer_select(sqlstr0)

    # 当前所有没有处理的
    try:
        sqlstr1 = "SELECT TOP 200 ReportCode, RepeatCode, Simhash FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE Simhash1 is NOT NULL AND ReportCode > '{}' order by ReportCode asc".format(content)
        result = execute_SqlServer_select(sqlstr1)
    except:
        return
    if result:
        for ReportCode, RepeatCode, Simhash in result:
            # # 之前
            # index = result0.index((ReportCode, RepeatCode, Simhash))
            # print index
            # if index > 0:
            #     new_RepeatCode = []
            #     fw = [Simhash[:16], Simhash[16:32], Simhash[32:48], Simhash[48:64]]
            #     for ReportCode2, RepeatCode2, Simhash2 in result0[:index]:
            #         # 四段有无一样的
            #         if Simhash[:16] in fw  or Simhash[16:32] in fw  or Simhash[32:48] in fw  or Simhash[48:64] in fw :
            #             simhash = SimHash()
            #             num = simhash.hammingDis(Simhash, Simhash2)
            #             # 小于海明距离的范围
            #             if num <= diatance:
            #                 print u'海明距离', num
            #                 new_RepeatCode.append(unicode(ReportCode2))
            #         if new_RepeatCode:
            #             new_RepeatCode1 = ','.join(new_RepeatCode)
            #             sql_updata = "UPDATE Report_ReportBaseInfo_Xbrl_copy3 SET RepeatCode = '{0}' WHERE ReportCode = '{1}'".format(
            #                 unicode(new_RepeatCode1), ReportCode)
            #             if execute_SqlServer_updata(sql_updata):
            #                 with open('error_hash.log', 'a+') as fp:
            #                     now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
            #                     fp.write(u'去重失败...{}'.format(now_time2) + '\n')
            #                     fp.write(sql_updata + u'更新失败...' + '\n')
            #                     fp.write('=' * 30 + '\n')

            # 之前
            index = result0.index((ReportCode, RepeatCode, Simhash))
            print index
            if index > 0:
                new_RepeatCode = []
                for ReportCode2, RepeatCode2, Simhash2 in result0[:index]:
                    simhash = SimHash()
                    num = simhash.hammingDis(Simhash, Simhash2)
                    # 小于海明距离的范围
                    if num <= diatance:
                        print u'海明距离', num
                        new_RepeatCode.append(unicode(ReportCode2))
                if new_RepeatCode:
                    new_RepeatCode1 = ','.join(new_RepeatCode)
                    sql_updata = "UPDATE Report_ReportBaseInfo_Xbrl_copy3 SET RepeatCode = '{0}' WHERE ReportCode = '{1}'".format(
                        unicode(new_RepeatCode1), ReportCode)
                    if execute_SqlServer_updata(sql_updata):
                        with open('error_hash.log', 'a+') as fp:
                            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                            fp.write(u'去重失败...{}'.format(now_time2) + '\n')
                            fp.write(sql_updata + u'更新失败...' + '\n')
                            fp.write('=' * 30 + '\n')
            # 记录
            with open('hash_code.log', 'w') as fp:
                now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
                fp.write(now_time2 + '\n' + str(ReportCode))
        print "去重[ok]"


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
    print content

    sqlstr1 = "SELECT TOP 200 ReportOriginal, ReportCode FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE Simhash1 is NULL AND ReportCode > '{}' order by ReportCode asc".format(content)
    result = execute_SqlServer_select(sqlstr1)
    simhash = SimHash()
    for ReportOriginal, ReportCode in result:
        # 记录
        with open('hash_select.log', 'w') as fp:
            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
            fp.write(now_time2 + '\n' + str(ReportCode))
        # 添加哈希
        path = S3(ReportOriginal)
        if path:
            key = simhash.get_simhash(path)
            print ReportCode, ReportOriginal
            print key
            os.remove(path)
            if key:
                sql_updata(key, ReportCode)
        # 记录
        with open('hash_select.log', 'w') as fp:
            now_time2 = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
            fp.write(now_time2 + '\n' + str(ReportCode))

    print '[ok]'


# 更新数据库
def sql_updata(Simhash, ReportCode):
    Simhash1 = Simhash[:16]
    Simhash2 = Simhash[16:32]
    Simhash3 = Simhash[32:48]
    Simhash4 = Simhash[48:64]
    sql_updata = "UPDATE Report_ReportBaseInfo_Xbrl_copy3 SET Simhash = '{0}', Simhash1 = '{1}', Simhash2 = '{2}', Simhash3 = '{3}', Simhash4 = '{4}'  WHERE ReportCode = '{5}'".format(Simhash, Simhash1, Simhash2,Simhash3, Simhash4, ReportCode)
    execute_SqlServer_updata(sql_updata)

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


def main():
    # sql_select()
    simhash = SimHash()

    # po = Pool(10)
    for dirpath, dirnames, filenames in os.walk(driver_path):
        for filename in filenames:
            index = filenames.index(filename)
            print '下标', index
            file_path1 = dirpath +'/'+ filename
            # word = simhash.news_process(file_path1)
            # print word
            # print proc_text(word)
            key1 = simhash.get_simhash(file_path1)
            for i in filenames[:index]:
                file_path2 = dirpath + '/' + i
                key2 = simhash.get_simhash(file_path2)
                a = simhash.hammingDis(key1,key2)
                print '海明距离', a
                print file_path1
                print key1
                print file_path2
                print key2


            # po.apply_async(run, (file_path, key))

#
#     # po.close()  # 关闭进程池，关闭后po不再接收新的请求
#     # po.join()
#     end = time.time()
#     # print(u'关闭数据库连接...')
#     # db_conn = SqlServer()
#     # cursor = db_conn.cursor()
#     # cursor.close()
#     # db_conn.close()
#     print(end - start)
#     print('[ok]')



if __name__ == '__main__':
    # sql_select()
    sql_select_simhash()
    # main()
    # S3('/report/2017/08/01/300005409296_20170801_1991EB.PDF')
    # sql_select_simhash()

    # simhash = SimHash()
    # key1 = simhash.get_simhash('./report/300005408489_20170801_3DFAC1.txt')
    # key2 = simhash.get_simhash('./report/300005408921_20170801_E62E4F.txt')
    # a = simhash.hammingDis(key1, key2)
    # print key1
    # print key2
    # print a
    '''
    0000000000000000000000000000000101011110111101000000000100101111
    0000000000000000000000000000000101011110111101000000011100101111
    '''