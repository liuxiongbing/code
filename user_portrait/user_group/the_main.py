#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
# ====#====#====#====  
# __author__ = "liu" 
# Time : 2020/6/24 10:26  
# FileName: the_main.py  
# Version: 1.0.0
# Describe:   用户分群
# ====#====#====#====
import traceback

from the_log import Logger
from the_utils import GroupUtils

if __name__ == '__main__':
    # 解析参数
    param = GroupUtils.get_param()
    try:
        # 执行并将数据插入至hive分群分区表
        the_logic = GroupUtils.excute_logic(param)
    except BaseException as e:
        log = Logger('%s.log' % param['groupName'], level='info')
        log.logger.error("\n" + traceback.format_exc())

# 注意:
# 一个脚本一张表
# 所有历史数据
# 用户的所有信息
# 用户在06点后手动刷新时，会产生今日数据，否则每天只能看到昨日数据


