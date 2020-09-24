#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
# ====#====#====#====  
# __author__ = "liu"
# Time : 2020/6/15 9:33
# FileName: generalduty_logic.py
# Version: 1.0.0
# Describe:  指标标签
# ====#====#====#====

import sys
import argparse
import traceback

from read_config import Getconf
from the_log import Logger
from the_utils import TheUtiles


if __name__ == '__main__':
    # 获取参数
    the_params = TheUtiles.get_param()
    tag_name = the_params['tagName']
    the_create = ""
    the_logic = ""
    try:
        if the_params['type'] == 0:
            # 获取建表语句
            the_create = TheUtiles.get_create(tag_name, "double")
            print(the_create)
            # 获取sql
            the_logic = TheUtiles.get_index_logic(the_params)
            print(the_logic)
        elif the_params['type'] == 1:
            # 获取建表语句
            the_create = TheUtiles.get_create(tag_name, "string")
            print(the_create)
            # 获取sql
            the_logic = TheUtiles.get_predilection_logic(the_params)
            print(the_logic)
        else:
            TheUtiles.send_email(
                " %s Script parameter content error,needs to be 0 or 1:(type=%s)" % (tag_name, the_params['type']))
            raise RuntimeError(
                "➤➤➤➤➤Pay attention to: type parameter content error,needs to be 0 or 1:(type=%s)" % the_params['type'])
        # 需要判断是否是新建标签,False为新建标签
        the_bool = TheUtiles.first_deal(tag_name)
        # 执行
        TheUtiles.excute_logic(the_create, the_logic, the_bool, tag_name, the_params['type'])
    except BaseException as e:
        log = Logger('%s.log' % tag_name, level='info')
        log.logger.error("\n" + traceback.format_exc())

