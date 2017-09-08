# -*- coding: utf-8 -*-

import os
import sys
import logging
import datetime
import pymssql
# python3.6不支持pymssql用这个包替换sql server连接
# import pyodbc

'''正式入库db块'''

# sql server
class SqlServer(object):
    def __new__(cls, *args, **kw):
            if not hasattr(cls, '_instance'):
                cls._instance = pymssql.connect(
                    host="",
                    database="XBRL",
                    user="crawl",
                    password="/7yk3aAe"
                )
            return cls._instance

    # def __new__(cls, *args, **kw):
    #     if not hasattr(cls, '_instance'):
    #         cls._instance = pyodbc.connect(
    #             'DRIVER={SQL Server};SERVER=;DATABASE=XBRL_TEMP;UID=crawl;PWD=/7yk3aAe'
    #         )
    #     return cls._instance


# 执行SqlServer中的count语句
def get_SqlServer_count(sqlstr):
    try:
        db_conn = SqlServer()
        cursor = db_conn.cursor()
        cursor.execute(sqlstr)
        result = cursor.fetchall()[0]
        # db_conn.commit()
        # db_conn.close()
        return result[0]
    except Exception as e:
        print('查询数量出错: {} SQL语句: {}'.format(e, sqlstr))


# 执行SqlServer中的select语句
def execute_SqlServer_select(sqlstr):
    try:
        db_conn = SqlServer()
        cursor = db_conn.cursor()
        cursor.execute(sqlstr)
        # 注意pymssql包列表里面是元祖！！！而pyodbc包列表里面是其他表现方式！！！
        result = cursor.fetchall()
        db_conn.commit()
        # db_conn.close()
        return result
    except Exception as e:
        print('查询出错: {} SQL语句: {}'.format(e, sqlstr))
        return None


# 执行SqlServer中的insert语句
def execute_SqlServer_insert(sqlstr):
    try:
        db_conn = SqlServer()
        cursor = db_conn.cursor()
        cursor.execute(sqlstr)
        db_conn.commit()
        # db_conn.close()
        print("插入成功...{}".format(sqlstr))
    except Exception as e:
        print('插入失败 {} SQL语句: {}'.format(e, sqlstr))

# 特殊！
def DB_insert_to_and_ReportCode(sqlstr, params):
    try:
        db_conn = SqlServer()
        cursor = db_conn.cursor()
        cursor.execute(sqlstr, params)
        result = cursor.fetchone()[0]
        db_conn.commit()  # 保存
        print("插入成功并返回结果...{}".format(result))
        return result
    except Exception as e:
        print('插入失败 {} SQL语句: {}'.format(e, sqlstr))


# 执行SqlServer中的updata语句
def execute_SqlServer_updata(sqlstr):
    try:
        db_conn = SqlServer()
        cursor = db_conn.cursor()
        cursor.execute(sqlstr)
        db_conn.commit()
        # db_conn.close()
        print("更新成功...{}".format(sqlstr))
    except Exception as e:
        print('更新失败 {} SQL语句: {}'.format(e, sqlstr))
        return True



if __name__ == '__main__':
    # 注意sql语句里面只能是单引号！

    # sql = "SELECT GUID FROM Report_ReportBaseInfo_Xbrl WHERE ReportCode = '{}'".format('300005479147')
    # a = execute_SqlServer_select(sql)[0][0]

    # sql2 = "SELECT ReportFileName FROM Report_ReportBaseInfo_Xbrl WHERE FileName = '{}'".format('ddd')
    # b = execute_SqlServer_select(sql2)
    sql3 = "SELECT ReportCode, ReportOriginal FROM Report_ReportBaseInfo_Xbrl WHERE FileName = '{}'".format('2017/08/30-142583')
    a, b = execute_SqlServer_select(sql3)
    # print type(a)
    print(a,b)