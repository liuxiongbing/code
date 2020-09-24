#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
# ====#====#====#====  
# __author__ = "liu" 
# Time : 2020/6/24 15:45  
# FileName: the_utils.py  
# Version: 1.0.0
# Describe:   
# ====#====#====#====
import argparse
import datetime
import json
import os
import smtplib
import time
from email.mime.text import MIMEText
from email.utils import formataddr


class GroupUtils:

    def __init__(self):
        pass

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
            GroupUtils.send_email("Parameter format error, script execution failed")
            raise RuntimeError("➤➤➤➤➤Pay attention to: Parameter resolution failed")
        group_name = the_rule['groupName']
        rule = the_rule['rule']
        # 0: 或; 1: 且
        condition_operator = rule['conditionOperator']
        # 条件数组
        conditions = rule['conditions']
        the_param = {'groupName': group_name, 'conditionOperator': condition_operator, 'conditions': conditions}
        return the_param

    @staticmethod
    def excute_logic(param):
        """
        :type param: 字典
        """
        raise RuntimeError("➤➤➤➤➤Pay attention to: Cluster table creation failed")
        # 获取表名
        group_name = param['groupName']
        # 获取所有标签字段与类型
        tag_filed = GroupUtils.get_tag_filed(group_name)
        # 建表,分区
        the_create = "impala-shell -B -q \" create external table if not exists default.%s(%s)  " \
                     "partitioned by (part_time string)  row format delimited fields terminated by '\001'  " \
                     "stored as parquet tblproperties ('parquet.compress'='SNAPPY'); \" 2>&1 " % (group_name, tag_filed[1])
        create_info = os.popen(the_create).read()
        if str(create_info).find("ERROR") != -1:
            GroupUtils.send_email('%s clustering script failed to build table' % group_name)
            raise RuntimeError("➤➤➤➤➤Pay attention to: Cluster table creation failed")
        if str(create_info).find("Table already exists") == -1:
            # 首次建群
            # 获取插入逻辑
            the_bool = True
            tmp_fil = []
            insert_logic = GroupUtils.get_insert_logic(group_name, param, tmp_fil, the_bool)
            print(insert_logic)
            insert_result = os.popen(insert_logic).read()
            if str(insert_result).find("ERROR") != -1:
                GroupUtils.send_email("%s clustering script data insertion failed,The cluster table will not have "
                                      "today's data" % group_name)
                raise RuntimeError("➤➤➤➤➤Pay attention to: Data insertion failure")
        else:
            # 非首次
            # 首次获取分群表的所有字段
            group_filed = GroupUtils.get_group_filed(group_name)
            group_filed = group_filed[0:-1]
            if len(group_filed) < len(tag_filed[0]):
                # 获取加字段逻辑
                add_col = GroupUtils.get_add_col(group_filed, tag_filed[0], group_name)
                print(add_col)
                add_result = os.popen(add_col).read()
                print(add_result)
                if str(add_result).find("New column(s) have been added to the table") == -1:
                    # 加字段失败
                    GroupUtils.send_email("%s clustering script add field failure,The cluster table will not have today's data" % group_name)
                    raise RuntimeError("➤➤➤➤➤Pay attention to: Add field failure")

            group_filed_ag = GroupUtils.get_group_filed(group_name)
            # 去除分区字段
            group_filed_ag = group_filed_ag[0:-1]
            # 获取插入逻辑
            the_bool = False
            insert_logic = GroupUtils.get_insert_logic(group_name, param, group_filed_ag, the_bool)
            insert_result = os.popen(insert_logic).read()
            if str(insert_result).find("ERROR") != -1:
                GroupUtils.send_email(
                    "%s clustering script data insertion failed,The cluster table will not have today's data" % group_name)
                raise RuntimeError("➤➤➤➤➤Pay attention to: Data insertion failure")

    @staticmethod
    def get_tag_filed(group_name):
        # 返回字典，字符串
        tag_type = "impala-shell -B --output_delimiter '|'  -q \"desc default.dws_user_tag;\""
        name_type = os.popen(tag_type).read()
        num = 1
        while name_type == "":
            num += 1
            if num == 100:
                GroupUtils.send_email(
                    "%s clustering script loops up to 100 times,Please check the user portrait table" % group_name)
                raise RuntimeError("➤➤➤➤➤Pay attention to: To 100 cycles")
            time.sleep(60)
            name_type = os.popen(tag_type).read()

        the_list = name_type[:-1].split("\n")
        tag_dic = {}
        filed_str = ""
        for tag in the_list:
            filed_list = tag.split("|")
            tag_dic[filed_list[0]] = filed_list[1]
            filed_str = filed_str + "%s %s," % (filed_list[0], filed_list[1])
        filed_str = filed_str[:-1]
        return tag_dic, filed_str

    @staticmethod
    def get_group_filed(group_name):
        # 返回数组
        filed_group = "impala-shell --print_header -B --output_delimiter '|' -q  \" select * from default.%s limit 0 \"" % group_name
        filed_str = os.popen(filed_group).read()
        filed_list = filed_str[:-1].split("|")
        return filed_list

    @staticmethod
    def get_insert_logic(group_name, param, filed, the_bool):
        # 返回字符串
        the_time = datetime.datetime.now()
        now_date = the_time - datetime.timedelta(days=1)
        if int(str(the_time)[11:13]) > 6:
            now_date = the_time
        # 拼接条件
        condition_operator = param['conditionOperator']
        conditions = param['conditions']
        # term不会为空
        term = ""
        if len(conditions) == 1:
            condition = conditions[0]
            value_type = condition['valueType']
            tag_name = condition['tagName']
            operator = condition['operator']
            if operator == 1 or operator == 2:
                value = condition['value']
                the_operator = "=" if operator == 1 else "!="
                va = "'%s'" if value_type == 1 else "%s"
                term = term + "%s %s " + va
                term = term % (tag_name, the_operator, value)
            elif operator == 3 or operator == 4:
                # like
                values = condition['values']
                the_operator = "like" if operator == 3 else "not like"
                for value in values:
                    term = term + tag_name + " %s " % the_operator + "'%" + str(value) + "%' and "
                term = term[:-5]
            elif operator == 5 or operator == 6:
                # null
                the_operator = "is" if operator == 5 else "is not"
                term = term + "%s %s null" % (tag_name, the_operator)
            elif operator == 7 or operator == 8:
                value = condition['value']
                the_operator = ">" if operator == 7 else "<"
                term = term + "%s %s %s" % (tag_name, the_operator, value)
            elif operator == 9:
                start_value = condition['startValue']
                end_value = condition['endValue']
                term = term + "%s > %s and %s < %s" % (tag_name, start_value, tag_name, end_value)
            else:
                GroupUtils.send_email(
                    " %s clustering Script parameter content error,needs to be 1 to 9:(operator=%s)" % (group_name, operator))
                raise RuntimeError(
                    "➤➤➤➤➤Pay attention to: operator parameter content error,needs to be 1 to 9:(propertyValueType=%s)" % operator)
        else:
            for condition in conditions:
                value_type = condition['valueType']
                tag_name = condition['tagName']
                operator = condition['operator']
                if operator == 1 or operator == 2:
                    value = condition['value']
                    the_operator = "=" if operator == 1 else "!="
                    va = "'%s'" if value_type == 1 else "%s"
                    term = term + "%s %s " + va + " %s "
                    term = term % (tag_name, the_operator, value, condition_operator)
                elif operator == 3 or operator == 4:
                    # like
                    values = condition['values']
                    the_operator = "like" if operator == 3 else "not like"
                    for value in values:
                        term = term + "(" + tag_name + " %s " % the_operator + "'%" + str(value) + "%' and "
                    term = term[:-5] + ")" + " %s " % condition_operator
                elif operator == 5 or operator == 6:
                    # null
                    the_operator = "is" if operator == 5 else "is not"
                    term = term + "%s %s null %s " % (tag_name, the_operator, condition_operator)
                elif operator == 7 or operator == 8:
                    value = condition['value']
                    the_operator = ">" if operator == 7 else "<"
                    term = term + "%s %s %s %s " % (tag_name, the_operator, value, condition_operator)
                elif operator == 9:
                    start_value = condition['startValue']
                    end_value = condition['endValue']
                    term = term + "(%s > %s and %s < %s) %s " % (tag_name, start_value, tag_name, end_value, condition_operator)
                else:
                    GroupUtils.send_email(
                        " %s clustering Script parameter content error,needs to be 1 to 9:(operator=%s)" % (group_name, operator))
                    raise RuntimeError(
                        "➤➤➤➤➤Pay attention to: operator parameter content error,needs to be 1 to 9:("
                        "propertyValueType=%s)" % operator)
            term = term[:-4]
        if the_bool:
            insert_sql = "impala-shell -q \"insert overwrite table %s partition(part_time='%s') select * from " \
                         "default.dws_user_tag where %s \" 2>&1 "
            insert_sql = insert_sql % (group_name, str(now_date)[0:10], term)
        else:
            insert_sql = "impala-shell -q \"insert overwrite table %s partition(part_time='%s') select %s from " \
                         "default.dws_user_tag where %s \" 2>&1 "
            gro = ""
            for fi in filed:
                gro = gro + "%s," % fi
            insert_sql = insert_sql % (group_name, str(now_date)[0:10], gro[:-1], term)
        return insert_sql

    @staticmethod
    def get_add_col(group_filed, tag_filed, group_name):
        # 返回字符串
        add_str = "impala-shell -q \" ALTER TABLE %s ADD COLUMNS (%s);\" "
        add_filed = ""
        tup_list = tag_filed.items()
        for tup in tup_list:
            if tup[0] not in group_filed:
                add_filed = add_filed + "%s %s," % (tup[0], tup[1])
        the_filed = add_filed[:-1]
        return add_str % (group_name, the_filed)

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
        GroupUtils.the_email(content, 'receive_email')
