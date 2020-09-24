#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import configparser
import os
import sys
import pymysql as pymysql


class Getconf:

    @staticmethod
    def getdb_con():
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
            print("connect database exception")
            print(Exception, exc)
            sys.exit(1)

    @staticmethod
    def readfile():
        conf_file = os.path.abspath('./pro.conf')
        if os.path.exists(conf_file):
            conf = configparser.ConfigParser()
            conf.read(conf_file, encoding='utf8')
            return conf
        else:
            print('configuration file read failed')
            sys.exit(1)

    @staticmethod
    def get_source_field():
        try:
            conf_file = os.path.abspath('./pro.conf')
            if os.path.exists(conf_file):
                conf = configparser.ConfigParser()
                conf.read(conf_file, encoding='utf8')
                return conf
            else:
                print('configuration file does not exist')
                sys.exit(1)
        except Exception as e:
            print("failed to get state")
            print(Exception, e)
            sys.exit(1)
