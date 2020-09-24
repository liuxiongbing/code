#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import configparser
import os
import sys
import pymysql as pymysql

from the_log import Logger


class Getconf:

    @staticmethod
    def getdb_con():
        log = Logger('merge.log', level='info')
        try:
            host = Getconf.readfile().get('db', 'host')
            port = int(Getconf.readfile().get('db', 'port'))
            user = Getconf.readfile().get('db', 'user')
            password = Getconf.readfile().get('db', 'password')
            database = Getconf.readfile().get('db', 'database')
            conn = pymysql.connect(host=host, user=user, password=password, port=port, database=database,
                                   charset="utf8")
            return conn
        except Exception as exc:
            log.logger.error("连接数据库异常")
            print("连接数据库异常")
            print(Exception, exc)
            sys.exit(1)

    @staticmethod
    def readfile():
        log = Logger('merge.log', level='info')
        conf_file = os.path.abspath('./pro.conf')
        if os.path.exists(conf_file):
            conf = configparser.ConfigParser()
            conf.read(conf_file, encoding='utf8')
            return conf
        else:
            log.logger.error("配置文件读取失败")
            print('配置文件读取失败')
            sys.exit(1)

