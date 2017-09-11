# coding: utf-8

import jieba
import numpy as np
import re,json
import os

import time

import threading
from multiprocessing.dummy import Pool as ThreadPool

from multiprocessing import Manager,Pool
from boto3 import Session

# file = '/report/w03/2017/08/01/300005408580_20170801_FB7E0C.pdf'


# Client初始化
access_key = ''
secret_key = ''

session = Session(access_key, secret_key, region_name='cn-north-1')
s3_client = session.client('s3')

datapool_path = 'report/w03/2017/08/01/300005408136_20170801_6AE15B.txt'

resp = s3_client.get_object(Bucket="datapool", Key=datapool_path)

with open('./report/10/300005408136_20170801_6AE15B.txt', 'wb') as f:
    f.write(resp['Body'].read())

