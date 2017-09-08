# coding: utf-8

from Tools import db
from Tools import db1


def sql_select():
    sqlstr="SELECT ReportDate,ReportSource,ReportOriginalTitle,ReportTitle,ReportSummary,ReportOriginal,ResearchInstitute,ReportSize, ReportPage,FileType,FileName,ReportFileName  FROM Report_ReportBaseInfo_Xbrl WHERE ReportDate >= '2017-08-01' and ReportDate < '2017-09-01'"
    result = db.execute_SqlServer_select(sqlstr)
    for i in result:
        sql_insert3(i)
        sql_insert4(i)
    print '[ok]'


def sql_insert3(params):
    sql_count = "SELECT COUNT(*) FROM Report_ReportBaseInfo_Xbrl_copy3 WHERE FileName = '{}'".format(params[10])  # 注意sql语句里面只能是单引号！
    file_count = db1.get_SqlServer_count(sql_count)

    sqlstr ="INSERT INTO Report_ReportBaseInfo_Xbrl_copy3(ReportDate,ReportSource,ReportOriginalTitle,ReportTitle,ReportSummary,ReportOriginal,ResearchInstitute,ReportSize, ReportPage,FileType,FileName,ReportFileName) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    if file_count == 0:
        try:
            db_conn = db1.SqlServer()
            cursor = db_conn.cursor()
            cursor.execute(sqlstr,params)
            db_conn.commit()
            # db_conn.close()
            print("插入成功...{}".format(sqlstr))
        except Exception as e:
            print('插入失败 {} SQL语句: {}'.format(e, sqlstr))


def sql_insert4(params):
    sql_count = "SELECT COUNT(*) FROM Report_ReportBaseInfo_Xbrl_copy4 WHERE FileName = '{}'".format(params[10])  # 注意sql语句里面只能是单引号！
    file_count = db1.get_SqlServer_count(sql_count)

    sqlstr ="INSERT INTO Report_ReportBaseInfo_Xbrl_copy4(ReportDate,ReportSource,ReportOriginalTitle,ReportTitle,ReportSummary,ReportOriginal,ResearchInstitute,ReportSize, ReportPage,FileType,FileName,ReportFileName) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    if file_count == 0:
        try:
            db_conn = db1.SqlServer()
            cursor = db_conn.cursor()
            cursor.execute(sqlstr,params)
            db_conn.commit()
            # db_conn.close()
            print("插入成功...{}".format(sqlstr))
        except Exception as e:
            print('插入失败 {} SQL语句: {}'.format(e, sqlstr))



if __name__ == '__main__':
    sql_select()