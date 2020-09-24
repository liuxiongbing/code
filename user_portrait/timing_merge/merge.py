#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
# ====#====#====#====  
# __author__ = "liu" 
# Time : 2020/6/18 9:33
# FileName: merge.py  
# Version: 1.0.0
# Describe:  用于合并执行完的标签脚本，固定时间执行此脚本
# ====#====#====#====
import traceback

from merge_utils import MergeLogic
from the_log import Logger

if __name__ == '__main__':
    try:
        # 获取mysql标签状态数据
        all_tag = MergeLogic.get_excute_info()
        if all_tag:
            # 执行逻辑
            MergeLogic.excute_logic(all_tag)
    except BaseException as e:
        log = Logger('merge.log', level='info')
        log.logger.error("\n" + traceback.format_exc())


# 注意:
# 画像表需先建好，初始与用户维一样
# 确定库，用户维字段需可配置
# 需注意其他集群所返回的信息是否与脚本中一样
# 画像表副本与其同一目录


