#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
# ====#====#====#====  
# __author__ = "liu"  
# FileName: *.py  
# Version:1.0.0  
# ====#====#====#====

import argparse
import datetime
import json
import os
import pymysql
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

from read_config import Getconf


class TheUtiles:

    def __init__(self):
        pass

    @staticmethod
    def get_create(tagname, the_type):
        tag_database = Getconf.readfile().get('dbs', 'tag')
        the_command = "impala-shell -B -q \" " \
                      "create external table if not exists %s.%s(user_id String,%s %s) " \
                      "row format delimited fields terminated by '\001' " \
                      "stored as parquet tblproperties ('parquet.compress'='SNAPPY'); \""
        return the_command % (tag_database, tagname, tagname, the_type)

    @staticmethod
    def get_index_logic(the_params):
        be_database = Getconf.readfile().get('dbs', 'behavior')
        body = the_params['body']
        event_code = body['eventCode']
        propertycode = body['propertyCode']
        # 0: 事件发生总次数; 1: 总和; 2: 最大值; 3: 最小值; 4: 均值; 5: 去重数。
        agg_operator = body['aggOperator']
        conditions = the_params['conditions']
        # 与或
        condition_operator = the_params['conditionOperator']
        tag_name = the_params['tagName']
        # 获取源字段的key
        # the_key = str(event_code) + "_" + str(propertycode) + "_" + str(agg_operator)
        # source_field = Getconf.get_source_field().get('source_field', str(the_key))
        # 拼接逻辑
        filed_sql = TheUtiles.fusion_operation(agg_operator, tag_name)
        filed_sql_time = TheUtiles.join_time(body, filed_sql, tag_name)
        filed_sql = filed_sql_time[0] % (propertycode, tag_name, be_database, event_code, filed_sql_time[1], filed_sql_time[2])
        the_logic = TheUtiles.join_rule(filed_sql, conditions, condition_operator, tag_name)
        return the_logic

    @staticmethod
    def get_predilection_logic(the_params):
        tag_database = Getconf.readfile().get('dbs', 'tag')
        be_database = Getconf.readfile().get('dbs', 'behavior')
        body = the_params['body']
        tag_name = the_params['tagName']
        conditions = the_params['conditions']
        condition_operator = the_params['conditionOperator']
        operator = body['operator']
        event_code = body['eventCode']
        condition_property = body['conditionProperty']
        topn = body['topN']
        time_sql = TheUtiles.join_time(body, "", tag_name)
        time_rule = TheUtiles.join_rule(time_sql[0] % (time_sql[1], time_sql[2]), conditions, condition_operator, tag_name)
        if operator == 0:
            target_property = body['targetProperty']
            the_insert = "impala-shell -B -q \" insert overwrite table %s.%s "
            numeric = """
                    select
                        the_two.user_id
                        ,group_concat(cast(the_two.%s as string),',') as %s
                    from
                        (
                            select
                                the_one.*
                            from
                                (
                                    select
                                        user_id
                                        ,%s
                                        ,%s
                                        ,ROW_NUMBER() over(PARTITION BY user_id order by %s desc) num
                                    from %s.dws_user_behavior
                                    where behavior_code='%s' %s
                                ) as the_one
                            where one.num<=%s
                        ) as the_two
                    group by the_two.user_id; \" 2>&1
                    """
            the_logic = the_insert % (tag_database, tag_name) + numeric % (condition_property, tag_name, target_property, condition_property, condition_property, be_database, event_code, time_rule[:-24], topn)
        elif operator == 1:
            the_insert = "impala-shell -B -q \" insert overwrite table %s.%s "
            the_text = """
                    select 
                        the_three.user_id
                        ,group_concat(distinct cast(the_three.%s as string),',') as %s 
                    from 
                        (
                            select 
                                the_two.* 
                            from 
                                (
                                    select 
                                        the_one.*
                                        ,DENSE_RANK() over(PARTITION BY the_one.user_id order by the_one.num desc) the_tank 
                                    from (
                                            select 
                                                user_id
                                                ,%s
                                                ,count(%s) over(PARTITION BY user_id,%s) num 
                                            from %s.dws_user_behavior 
                                            where behavior_code='%s' %s
                                        ) as the_one
                                ) the_two 
                            where the_two.the_tank<=%s
                        ) the_three
                    group by the_three.user_id; \" 2>&1
                    """
            the_logic = the_insert % (tag_database, tag_name) + the_text % (condition_property, tag_name, condition_property, condition_property, condition_property, be_database, event_code, time_rule[:-24], topn)
        else:
            TheUtiles.send_email(
                " %s Script parameter content error,needs to be 0 to 1:(Operator=%s)" % (tag_name, operator))
            raise RuntimeError(
                "➤➤➤➤➤Pay attention to: Operator parameter content error,needs to be 0 to 1:(Operator=%s)" % operator)
        return the_logic

    @staticmethod
    def excute_logic(the_create, the_logic, the_bool, tagname, the_type):
        # 连接mysql,0为正在执行，删除键否，1则相反
        # 判断，有则更改状态为正在执行，删除键为否

        # 无则插入正在执行，删除键为否
        # 不管是否执行成功，更改状态为执行完毕，删除键为是
        if the_create == "" or the_logic == "":
            TheUtiles.last_deal(the_bool, tagname)
            TheUtiles.send_email('%s Script:The execute statement is empty' % tagname)
            raise RuntimeError("➤➤➤➤➤Pay attention to: The execute statement is empty")
        create_table = os.popen(the_create).read()
        str_result = str(create_table)
        if str_result.find("Table already exists") != -1 or str_result.find("Table has been created") != -1:
            excute = os.popen(the_logic.encode("utf-8")).read()
            if str(excute).find("ERROR") == -1:
                # 判断是否新建标签
                if not the_bool:
                    # 执行合并逻辑
                    TheUtiles.merge_logic(the_bool, tagname, the_type)
                else:
                    TheUtiles.last_deal(the_bool, tagname)
            else:
                # 通知
                TheUtiles.last_deal(the_bool, tagname)
                TheUtiles.send_email('%s Script logic execution failed' % tagname)
                raise RuntimeError("➤➤➤➤➤Pay attention to: logic execution failed")
        else:
            # 通知
            TheUtiles.last_deal(the_bool, tagname)
            TheUtiles.send_email('%s Script table building failed' % tagname)
            raise RuntimeError("➤➤➤➤➤Pay attention to: failed to build table")

    # 获取传入参数
    @staticmethod
    def get_param():
        # 参数
        parser = argparse.ArgumentParser(
            description="When you call the script, pass in the JSON rule parameter with -the_rules")
        parser.add_argument("the_rules")
        args = parser.parse_args()
        try:
            the_rule = json.loads(args.the_rules)
        except Exception as exc:
            TheUtiles.send_email("Parameter format error, script execution failed")
            raise RuntimeError("➤➤➤➤➤Pay attention to: Parameter resolution failed")
        tag_name = the_rule['tagName']
        # 0: 指标; 1: 偏好
        the_type = the_rule['type']
        rule = the_rule['rule']
        body = rule['body']
        # 0: 或; 1: 且
        condition_operator = rule['conditionOperator']
        # 条件数组
        conditions = rule['conditions']
        the_param = {'tagName': tag_name, 'type': the_type, 'conditionOperator': condition_operator,
                     'conditions': conditions, 'body': body}
        return the_param

    # 时间
    @staticmethod
    def join_time(body, filed_sql, tag_name):
        time_type = body['timeType']
        filed_sql = filed_sql + " and event_time>='%s' and event_time<='%s' and ("
        # 现在
        the_now = datetime.datetime.now()
        # 本月
        this_m_s = datetime.datetime(the_now.year, the_now.month, 1)
        # 本年
        this_y_s = datetime.datetime(the_now.year, 1, 1)
        if time_type == 0:
            relative_time_type = body['relativeTimeType']
            if relative_time_type is None:
                past_ndays_start = body['pastNDaysStart']
                past_ndays_end = body['pastNDaysEnd']
                if past_ndays_start is not None and past_ndays_end is not None:
                    start_day = the_now - datetime.timedelta(days=past_ndays_start)
                    past_day = the_now - datetime.timedelta(days=past_ndays_end)
                    start_time = datetime.datetime(start_day.year, start_day.month, start_day.day, 00, 00, 00)
                    end_time = datetime.datetime(past_day.year, past_day.month, past_day.day, 23, 59, 59)
                else:
                    TheUtiles.send_email(
                        " %s Script parameter content error, Parameters pastNDaysStart and pastNDaysEnd are missing" % tag_name)
                    raise RuntimeError(
                        "➤➤➤➤➤Pay attention to: Parameters pastNDaysStart and pastNDaysEnd are missing")
            else:
                if relative_time_type == 2:
                    # 昨天
                    last_day = the_now - datetime.timedelta(days=1)
                    start_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 00, 00, 00)
                    end_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 23, 59, 59)
                elif relative_time_type == 3:
                    # 本周
                    this_w_s = the_now - datetime.timedelta(days=the_now.weekday())
                    this_w_e = the_now + datetime.timedelta(days=6 - the_now.weekday())
                    start_time = datetime.datetime(this_w_s.year, this_w_s.month, this_w_s.day, 00, 00, 00)
                    end_time = datetime.datetime(this_w_e.year, this_w_e.month, this_w_e.day, 23, 59, 59)
                elif relative_time_type == 4:
                    # 上周
                    last_w_s = the_now - datetime.timedelta(days=the_now.weekday() + 7)
                    last_w_e = the_now - datetime.timedelta(days=the_now.weekday() + 1)
                    start_time = datetime.datetime(last_w_s.year, last_w_s.month, last_w_s.day, 00, 00, 00)
                    end_time = datetime.datetime(last_w_e.year, last_w_e.month, last_w_e.day, 23, 59, 59)
                elif relative_time_type == 5:
                    # 本月
                    start_time = this_m_s
                    end_time = datetime.datetime(the_now.year, the_now.month + 1, 1) - datetime.timedelta(days=1)
                elif relative_time_type == 6:
                    # 上月
                    end_time = this_m_s - datetime.timedelta(days=1)
                    start_time = datetime.datetime(end_time.year, end_time.month, 1)
                elif relative_time_type == 7:
                    # 本年
                    start_time = this_y_s
                    end_time = datetime.datetime(the_now.year + 1, 1, 1) - datetime.timedelta(days=1)
                elif relative_time_type == 8:
                    # 去年
                    end_time = this_y_s - datetime.timedelta(days=1)
                    start_time = datetime.datetime(end_time.year, 1, 1)
                elif relative_time_type == 9:
                    # 过去7天
                    eight_day = the_now - datetime.timedelta(days=8)
                    last_day = the_now - datetime.timedelta(days=1)
                    start_time = datetime.datetime(eight_day.year, eight_day.month, eight_day.day, 00, 00, 00)
                    end_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 23, 59, 59)
                elif relative_time_type == 10:
                    # 过去30天
                    eight_day = the_now - datetime.timedelta(days=31)
                    last_day = the_now - datetime.timedelta(days=1)
                    start_time = datetime.datetime(eight_day.year, eight_day.month, eight_day.day, 00, 00, 00)
                    end_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 23, 59, 59)
                elif relative_time_type == 11:
                    # 过去90天
                    eight_day = the_now - datetime.timedelta(days=91)
                    last_day = the_now - datetime.timedelta(days=1)
                    start_time = datetime.datetime(eight_day.year, eight_day.month, eight_day.day, 00, 00, 00)
                    end_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 23, 59, 59)
                else:
                    TheUtiles.send_email(
                        " %s Script parameter content error,needs to be 2 to 11:(relativeTimeType=%s)" % (tag_name, relative_time_type))
                    raise RuntimeError(
                        "➤➤➤➤➤Pay attention to: relativeTimeType parameter content error,needs to be 2 to 11:("
                        "relativeTimeType=%s)" % relative_time_type)
        elif time_type == 1:
            start_time = body['startTime']
            past_ndays_end = body['pastNDaysEnd']
            end_time = the_now - datetime.timedelta(days=past_ndays_end)
        elif time_type == 2:
            start_time = body['startTime']
            end_time = body['endTime']
        else:
            TheUtiles.send_email(
                " %s Script parameter content error,needs to be 2 to 11:(timeType=%s)" % (tag_name, time_type))
            raise RuntimeError(
                "➤➤➤➤➤Pay attention to: timeType parameter content error,needs to be 0 to 2:(timeType=%s)" % time_type)
        return filed_sql, start_time, end_time

    # 指标标签头部sql
    @staticmethod
    def fusion_operation(agg_operator, tag_name):
        tag_database = Getconf.readfile().get('dbs', 'tag')

        the_insert = "impala-shell -B -q \" insert overwrite table %s.%s "
        if agg_operator == 0:
            filed_sql = "select user_id,count(%s) as %s from %s.dws_user_behavior where user_id!='NULL' and behavior_code='%s'"
        elif agg_operator == 1:
            filed_sql = "select user_id,sum(%s) as %s from %s.dws_user_behavior where user_id!='NULL' and behavior_code='%s'"
        elif agg_operator == 2:
            filed_sql = "select user_id,max(%s) as %s from %s.dws_user_behavior where user_id!='NULL' and behavior_code='%s'"
        elif agg_operator == 3:
            filed_sql = "select user_id,min(%s) as %s from %s.dws_user_behavior where user_id!='NULL' and behavior_code='%s'"
        elif agg_operator == 4:
            filed_sql = "select user_id,avg(%s) as %s from %s.dws_user_behavior where user_id!='NULL' and behavior_code='%s'"
        else:
            TheUtiles.send_email(
                " %s Script parameter content error,needs to be 0 to 4:(aggOperator=%s)" % (tag_name, agg_operator))
            raise RuntimeError(
                "➤➤➤➤➤Pay attention to: aggOperator parameter content error,needs to be 0 to 4:(aggOperator=%s)" % agg_operator)
        return the_insert % (tag_database, tag_name) + filed_sql

    # 规则
    @staticmethod
    def join_rule(filed_sql, conditions, condition_operator, tag_name):
        if conditions:
            the_len = len(conditions)
            for condition in conditions:
                property_code = condition['propertyCode']
                # 1: 等于; 2: 不等于;  7: 大于; 8: 小于; 9: 区间;
                operator = condition['operator']
                # 0: 数值; 1: 文本;
                property_value_type = condition['propertyValueType']
                value = condition['value']

                if operator == 1 or operator == 2:
                    actual_o = "=" if operator == 1 else "!="
                    if the_len == 1:
                        if property_value_type == 0:
                            next_sql = "%s" % property_code + "%s%s     "
                            filed_sql = filed_sql + next_sql % (actual_o, value)
                        elif property_value_type == 1:
                            next_sql = "%s" % property_code + "%s'%s'     "
                            filed_sql = filed_sql + next_sql % (actual_o, value)
                        else:
                            TheUtiles.send_email(
                                " %s Script parameter content error,needs to be 0 or 1:(propertyValueType=%s)" % (tag_name, property_value_type))
                            raise RuntimeError(
                                "➤➤➤➤➤Pay attention to: propertyValueType parameter content error,needs to be 0 or 1:(propertyValueType=%s)" % property_value_type)
                    else:
                        join_condition = "and" if condition_operator == 1 else "or"
                        if property_value_type == 0:
                            next_sql = "%s" % property_code + "%s%s %s     "
                            filed_sql = filed_sql + next_sql % (actual_o, value, join_condition)
                        elif property_value_type == 1:
                            next_sql = "%s" % property_code + "%s'%s' %s     "
                            filed_sql = filed_sql + next_sql % (actual_o, value, join_condition)
                        else:
                            TheUtiles.send_email(
                                " %s Script parameter content error,needs to be 0 or 1:(propertyValueType=%s)" % (tag_name, property_value_type))
                            raise RuntimeError(
                                "➤➤➤➤➤Pay attention to: propertyValueType parameter content error,needs to be 0 or 1:(propertyValueType=%s)" % property_value_type)
                elif operator == 7 or operator == 8:
                    if the_len == 1:
                        # 值
                        value = condition['value']
                        actual_o = ">" if operator == 1 else "<"
                        next_sql = "%s" % property_code + "%s%s     "
                        filed_sql = filed_sql + next_sql % (actual_o, value)
                    else:
                        join_condition = "and" if condition_operator == 1 else "or"
                        # 值
                        value = condition['value']
                        actual_o = ">" if operator == 1 else "<"
                        next_sql = "%s" % property_code + "%s%s %s "
                        filed_sql = filed_sql + next_sql % (actual_o, value, join_condition)
                elif operator == 9:
                    if the_len == 1:
                        # 区间值
                        start_value = condition['startValue']
                        end_value = condition['endValue']
                        next_sql = "(%s" % property_code + ">=%s and " + "%s" % property_code + "<=%s)     "
                        filed_sql = filed_sql + next_sql % (start_value, end_value)
                    else:
                        join_condition = "and" if condition_operator == 1 else "or"
                        # 区间值
                        start_value = condition['startValue']
                        end_value = condition['endValue']
                        next_sql = "(%s" % property_code + ">=%s and " + "%s" % property_code + "<=%s) %s "
                        filed_sql = filed_sql + next_sql % (start_value, end_value, join_condition)
                else:
                    TheUtiles.send_email(
                        " %s Script parameter content error,needs to be 1,2,7,8,9:(operator=%s)" % (tag_name, operator))
                    raise RuntimeError(
                        "➤➤➤➤➤Pay attention to: Operator parameter content error,needs to be 1,2,7,8,9:(operator=%s)" % operator)
            filed_sql = filed_sql[:-4] + ") group by user_id; \" 2>&1"
        else:
            filed_sql = filed_sql[:-5] + " group by user_id; \" 2>&1"
        return filed_sql

    # 状态处理
    @staticmethod
    def first_deal(tag_name):
        # 连接mysql,0为正在执行，否
        # 判断，有则更改状态为正在执行，删除键为否
        # 不管是否执行成功，更改状态为执行完毕

        # 无则插入正在执行，删除键为否
        # 不管是否执行成功，更改状态为执行完毕，删除键为是
        conn = Getconf.getdb_con()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = "select tag_name from tag_merge where tag_name='%s'" % tag_name
        count = cursor.execute(sql)
        the_bool = False if count == 0 else True
        if the_bool:
            # 更改状态为正在执行，删除键为否
            update_sql = "UPDATE tag_merge SET the_status=0,is_delete=0 WHERE tag_name='%s'" % tag_name
            cursor.execute(update_sql)
            conn.commit()
        else:
            # 插入正在执行，删除键为否
            insert_sql = "INSERT INTO tag_merge(tag_name,the_status,is_delete) VALUES('%s',0,0)" % tag_name
            cursor.execute(insert_sql)
            conn.commit()
        conn.close()
        return the_bool

    # 状态处理
    @staticmethod
    def last_deal(the_bool, tag_name):
        conn = Getconf.getdb_con()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        if the_bool:
            # 更改状态为执行完毕
            update_sql = "UPDATE tag_merge SET the_status=1 WHERE tag_name='%s'" % tag_name
            cursor.execute(update_sql)
            conn.commit()
        else:
            # 更改状态为执行完毕，删除键为是
            update_sql = "UPDATE tag_merge SET the_status=1,is_delete=1 WHERE tag_name='%s'" % tag_name
            cursor.execute(update_sql)
            conn.commit()
        conn.close()

    # 新建标签合并，注意多个用户的情况！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
    @staticmethod
    def merge_logic(the_bool, tag_name, the_type):
        user_database = Getconf.readfile().get('dbs', 'UserProfile')
        dim_database = Getconf.readfile().get('dbs', 'user')
        tag_database = Getconf.readfile().get('dbs', 'tag')
        tmp_database = Getconf.readfile().get('dbs', 'UserProfile_tmp')
        tmp_dir = Getconf.readfile().get('dir', 'UserProfile_tmp')
        user_dir = Getconf.readfile().get('dir', 'UserProfile')
        # 获取所有标签及其类型，判断有无画像表
        tag_type = "impala-shell -B --output_delimiter '|'  -q \"desc %s.dws_user_tag;\"" % user_database
        name_type = os.popen(tag_type).read()
        the_list = name_type[:-1].split("\n")
        # 合并sql，此处需加入用户维表
        merge_sql = "select "
        # 需排除的字段
        the_filed = Getconf.readfile().get('dim', 'filed')
        exclude_filed = the_filed.split(",")
        for mer in exclude_filed:
            merge_sql = merge_sql + "a.%s," % mer
        exclude_filed.append(tag_name)
        for tag in the_list:
            filed_list = tag.split("|")
            if filed_list[0] not in exclude_filed:
                merge_sql = merge_sql + "b.%s," % filed_list[0]
        if merge_sql.endswith(","):
            merge_sql = merge_sql + "c.%s  from %s.dim_user as a left join %s.dws_user_tag as b " \
                                         "on a.user_id=b.user_id left join %s.%s as c on a.user_id=c.user_id; \" 2>&1" % (tag_name, dim_database, user_database, tag_database, tag_name)
        else:
            merge_sql = merge_sql + ",c.%s  from %s.dim_user as a left join %s.dws_user_tag as b " \
                                         "on a.user_id=b.user_id left join %s.%s as c on a.user_id=c.user_id; \" 2>&1" % (tag_name, dim_database, user_database, tag_database, tag_name)

        delete_tmp_table = "impala-shell -q \" drop table %s.dws_user_tag_tmp \"" % tmp_database
        the_first = os.popen(delete_tmp_table).read()
        first_tmp = os.popen("hdfs dfs -rm -r %s/dws_user_tag_tmp 2>&1" % tmp_dir).read()
        the_tmp_create = "impala-shell -B -q \" create external table %s.dws_user_tag_tmp(" % tmp_database
        # 新建临时表并合并
        for li in the_list:
            tag_and_type = li.split("|")
            the_tmp_create = the_tmp_create + "%s %s," % (tag_and_type[0], tag_and_type[1])
        d = "string"
        if the_type == 0:
            d = "double"
        the_tmp_create = the_tmp_create + "%s %s) row format delimited fields terminated by '\001' " \
                                          "stored as parquet tblproperties ('parquet.compress'='SNAPPY');  " % (tag_name, d)
        the_tmp_create = the_tmp_create + "  insert overwrite table %s.dws_user_tag_tmp  " % tmp_database + merge_sql

        # 新建标签表
        tag_create = "impala-shell -B -q \" create external table %s.dws_user_tag(" % user_database
        for li in the_list:
            tag_and_type = li.split("|")
            tag_create = tag_create + "%s %s," % (tag_and_type[0], tag_and_type[1])
        tag_create = tag_create[:-1] + ") row format delimited fields terminated by '\001' stored as parquet tblproperties ('parquet.compress'='SNAPPY');  "

        # 建表并插入数据至临时表
        result = os.popen(the_tmp_create).read()

        if str(result).find("Table has been created") != -1 and str(result).find("ERROR") == -1:
            # 成功
            delete_table = "impala-shell -q \" drop table %s.dws_user_tag \"" % user_database
            one = os.popen(delete_table).read()
            if str(one).find("Table has been dropped") == -1:
                TheUtiles.last_deal(the_bool, tag_name)
                TheUtiles.send_email('%s script failed to delete table, this tag will not exist in the portrait table' % tag_name)
                raise RuntimeError("➤➤➤➤➤Pay attention to: Delete the table failed")

            # 改名，并判断是否有今日数据，有则删，合并脚本中一样！
            now_time = datetime.datetime.now()
            name_suffix = str(now_time)[:10] + "_" + str(now_time)[11:13] + "_" + str(now_time)[14:16] + "_" + str(now_time)[17:19] + "_" + str(now_time)[20:22]
            # 判断目录存在
            is_exit = os.popen("hadoop fs -find %s/ -iname 'dws_user_tag_%s*'" % (user_dir, str(now_time)[:10])).read()
            two = os.popen(
                "hdfs dfs -mv %s/dws_user_tag %s/dws_user_tag_%s;echo $?" % (user_dir, user_dir, name_suffix)).read()
            exit_list = os.popen(
                "hadoop fs -find %s/ -iname 'dws_user_tag_%s*'" % (user_dir, str(now_time)[:10])).read()
            if two[:-1] == "0" and is_exit[:-1] != "":
                # 存在
                dir_list = exit_list[:-1].split('\n')
                dir_one = dir_list[0][-11:-9] + dir_list[0][-8:-6] + dir_list[0][-5:-3] + dir_list[0][-2:]
                dir_two = dir_list[1][-11:-9] + dir_list[1][-8:-6] + dir_list[1][-5:-3] + dir_list[1][-2:]
                rf = ""
                if int(dir_two) > int(dir_one):
                    rf = os.popen("hdfs dfs -rm -r %s 2>&1" % dir_list[0]).read()
                else:
                    rf = os.popen("hdfs dfs -rm -r %s 2>&1" % dir_list[1]).read()
                if str(rf).find("INFO fs.TrashPolicyDefault: Moved") == -1:
                    TheUtiles.send_email("First attempt to delete today's extra copies failed")

            # 失败则新建表并插入数据
            three = os.popen("impala-shell -q \" ALTER TABLE %s.dws_user_tag_tmp RENAME TO %s.dws_user_tag \" 2>&1" % (tmp_database, user_database)).read()
            four = os.popen("hdfs dfs -mv %s/dws_user_tag_tmp %s/dws_user_tag" % (tmp_dir, user_dir)).read()
            ip = Getconf.readfile().get('hdfs', 'ip')
            # 映射失败则标签表中无数据
            five = os.popen("impala-shell -q \" alter table %s.dws_user_tag set location 'hdfs://%s:8020%s/dws_user_tag' \" 2>&1" % (user_database, ip, user_dir)).read()
            TheUtiles.last_deal(the_bool, tag_name)
            if str(three).find("Renaming was successful") == -1 or str(five).find("New location has been set") == -1:
                TheUtiles.send_email("The first merge %s script fails,The portrait table will reset to the previous data" % tag_name)
                # 删除画像表,及其目录，新建并插入数据
                delete_tag = "impala-shell -q \" drop table %s.dws_user_tag \"" % user_database
                os.popen(delete_tag)
                os.popen("hdfs dfs -rm -r %s/dws_user_tag" % user_dir)
                # 使用load
                old_tag = os.popen(tag_create).read()
                load_ok = os.popen("hive -e \"load data  inpath '%s/dws_user_tag_%s/*' overwrite into table %s.dws_user_tag\" 2>&1" % (user_dir, name_suffix, user_database)).read()
                six = os.popen("impala-shell -q \" refresh  %s.dws_user_tag \"" % user_database)
                sev = os.popen("hdfs dfs -cp %s/dws_user_tag/*  %s/dws_user_tag_%s " % (user_dir, user_dir, name_suffix))
                if str(old_tag).find("Table has been created") == -1 or str(load_ok).find("FAILED") != -1:
                    TheUtiles.send_email('Reset failed for portrait table!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    raise RuntimeError("➤➤➤➤➤Pay attention to: Reset failed for portrait table!")
        else:
            # 失败
            TheUtiles.last_deal(the_bool, tag_name)
            TheUtiles.send_email('The first merge %s script fails' % tag_name)
            raise RuntimeError("➤➤➤➤➤Pay attention to: The first merge %s script fails" % tag_name)

    @staticmethod
    def the_email(content, my_user):
        my_sender = 'send_email'
        my_pass = '替代密码'

        try:
            msg = MIMEText(content, 'plain', 'utf-8')
            msg['From'] = formataddr(["一拳超人", my_sender])
            msg['To'] = formataddr(["开发", my_user])
            msg['Subject'] = "Script exception"

            server = smtplib.SMTP_SSL("smtp.163.com", 465)
            server.login(my_sender, my_pass)
            server.sendmail(my_sender, [my_user, ], msg.as_string())
            server.quit()
        except Exception:
            raise RuntimeError("➤➤➤➤➤Pay attention to: Email failed to send")

    @staticmethod
    def send_email(content):
        TheUtiles.the_email(content, 'receive_email')

