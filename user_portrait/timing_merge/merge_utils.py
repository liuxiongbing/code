#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
# ====#====#====#====  
# __author__ = "liu" 
# Time : 2020/6/18 9:44  
# FileName: merge_utils.py  
# Version: 1.0.0
# Describe:   0为正在执行，未删除
# ====#====#====#====
import datetime
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import pymysql

from read_config import Getconf


class MergeLogic:

    @staticmethod
    def get_excute_info():
        name_status = " 'the_status': 0 "
        info = []
        get_sql = 'select tag_name,the_status from tag_merge where is_delete=0 order by update_time desc'
        while name_status.find("'the_status': 0") != -1:
            conn = Getconf.getdb_con()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            cursor.execute(get_sql)
            info = cursor.fetchall()
            name_status = str(info)
            time.sleep(10)
            conn.close()
        tag_list = []
        for name in info:
            tag_list.append(name['tag_name'])
        return tag_list

    @staticmethod
    def excute_logic(all_tag):
        tmp_database = Getconf.readfile().get('dbs', 'UserProfile_tmp')
        user_database = Getconf.readfile().get('dbs', 'UserProfile')
        tag_database = Getconf.readfile().get('dbs', 'tag')
        dim_database = Getconf.readfile().get('dbs', 'user')
        tmp_dir = Getconf.readfile().get('dir', 'UserProfile_tmp')
        user_dir = Getconf.readfile().get('dir', 'UserProfile')
        delete_tmp_table = "impala-shell -q \" drop table %s.dws_user_tag_tmp \""
        the_first = os.popen(delete_tmp_table % tmp_database).read()
        first_tmp = os.popen("hdfs dfs -rm -r %s/dws_user_tag_tmp" % tmp_dir).read()
        # 获取画像字段及类型
        # 临时表,与画像表完全一样,用户、标签、画像
        the_create = "impala-shell -B -q \" create external table %s.%s("
        t_filed = ""
        tag_type = "impala-shell -B --output_delimiter '|'  -q \"desc %s.dws_user_tag;\"" % user_database
        name_type = os.popen(tag_type).read()
        the_list = name_type[:-1].split("\n")
        the_filed = Getconf.readfile().get('dim', 'filed')
        exclude_filed = the_filed.split(",")
        for li in the_list:
            tag_and_type = li.split("|")
            if tag_and_type[0] in exclude_filed:
                t_filed = t_filed + "%s %s," % (tag_and_type[0], tag_and_type[1])

        fu = {}
        for li in the_list:
            tag_and_type = li.split("|")
            fu[tag_and_type[0]] = tag_and_type[1]

        for li in all_tag:
            t_type = fu[li]
            t_filed = t_filed + "%s %s," % (li, t_type)

        for li in the_list:
            tag_and_type = li.split("|")
            if tag_and_type[0] not in exclude_filed and tag_and_type[0] not in all_tag:
                t_filed = t_filed + "%s %s," % (tag_and_type[0], tag_and_type[1])
        the_create = the_create % (tmp_database, "dws_user_tag_tmp") + t_filed[:-1] + ") row format delimited fields terminated by '\001' stored as parquet tblproperties ('parquet.compress'='SNAPPY'); "

        # 此处需加入用户维表
        merge_sql = " select "
        the_join = " left join "
        # 需排除用户维的字段
        for mer in exclude_filed:
            merge_sql = merge_sql + "a.%s," % mer
        for tag in all_tag:
            if tag not in exclude_filed:
                merge_sql = merge_sql + "%s.%s," % (tag, tag)
                the_join = the_join + " %s.%s on a.user_id=%s.user_id left join " % (tag_database, tag, tag)
        # 第三张表
        for third in the_list:
            tag_and_type = third.split("|")
            third_tag = tag_and_type[0]
            if third_tag not in exclude_filed and third_tag not in all_tag:
                merge_sql = merge_sql + "dut.%s," % third_tag
        the_join = the_join + " %s.dws_user_tag as dut on a.user_id=dut.user_id; \" 2>&1" % user_database
        merge_sql = merge_sql[:-1] + "  from %s.dim_user as a " % dim_database
        merge_sql = " insert overwrite table %s.dws_user_tag_tmp " % tmp_database + merge_sql + the_join
        the_last_sql = the_create + merge_sql
        print(the_last_sql)
        # 建表并插入数据至临时表
        result = os.popen(the_last_sql).read()
        print(result)
        if str(result).find("Table has been created") != -1 and str(result).find("ERROR") == -1:
            # 成功
            delete_table = "impala-shell -q \" drop table %s.dws_user_tag \"" % user_database
            one = os.popen(delete_table).read()
            if str(one).find("Table has been dropped") == -1:
                MergeLogic.update_mysql(all_tag)
                MergeLogic.send_email(
                    'merge script failed to delete table, Portrait table data not updated')
                raise RuntimeError("➤➤➤➤➤Pay attention to: Delete the table failed")

            # 改名，并判断是否有今日数据与三天前副本，有则删
            now_time = datetime.datetime.now()
            three_ago = now_time - datetime.timedelta(days=3)
            name_suffix = str(now_time)[:10] + "_" + str(now_time)[11:13] + "_" + str(now_time)[14:16] + "_" + str(now_time)[17:19] + "_" + str(now_time)[20:22]
            # 判断目录存在
            is_exit = os.popen("hadoop fs -find %s/ -iname 'dws_user_tag_%s*'" % (user_dir, str(now_time)[:10])).read()
            two = os.popen(
                "hdfs dfs -mv %s/dws_user_tag %s/dws_user_tag_%s;echo $?" % (user_dir, user_dir, name_suffix)).read()
            exit_list = os.popen(
                "hadoop fs -find %s/ -iname 'dws_user_tag_%s*'" % (user_dir, str(now_time)[:10])).read()
            three_exit = os.popen("hadoop fs -find %s/ -iname 'dws_user_tag_%s*'" % (user_dir, str(three_ago)[:10])).read()
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
                    MergeLogic.send_email('Deleting extra copies today failed')
            if three_exit != "":
                # 存在
                rm = os.popen("hdfs dfs -rm -r %s 2>&1" % three_exit[:-1]).read()
                if str(rm).find("INFO fs.TrashPolicyDefault: Moved") == -1:
                    MergeLogic.send_email('Failed to delete the first three days of the copy')

            # 失败则新建表并插入数据
            three = os.popen(
                "impala-shell -q \" ALTER TABLE %s.dws_user_tag_tmp RENAME TO %s.dws_user_tag \" 2>&1" % (tmp_database, user_database)).read()
            four = os.popen(
                "hdfs dfs -mv %s/dws_user_tag_tmp %s/dws_user_tag" % (tmp_dir, user_dir)).read()
            ip = Getconf.readfile().get('hdfs', 'ip')
            # 映射失败则标签表中无数据
            five = os.popen(
                "impala-shell -q \" alter table %s.dws_user_tag set location 'hdfs://%s:8020%s/dws_user_tag' \" 2>&1" % (user_database, ip, user_dir)).read()
            MergeLogic.update_mysql(all_tag)
            if str(three).find("Renaming was successful") == -1 or str(five).find("New location has been set") == -1:
                MergeLogic.send_email(
                    "The merge script fails,The portrait table will reset to the previous data")
                # 删除画像表,及其目录，新建并插入数据
                delete_tag = "impala-shell -q \" drop table %s.dws_user_tag \"" % user_database
                os.popen(delete_tag)
                os.popen("hdfs dfs -rm -r %s/dws_user_tag" % user_dir)
                # 使用load
                old_tag = os.popen(the_create % "dws_user_tag").read()
                load_ok = os.popen(
                    "hive -e \"load data  inpath '%s/dws_user_tag_%s/*' overwrite into table %s.dws_user_tag\" 2>&1" % (user_dir, name_suffix, user_database)).read()
                six = os.popen("impala-shell -q \" refresh  %s.dws_user_tag \"" % user_database)
                sev = os.popen(
                    "hdfs dfs -cp %s/dws_user_tag/*  %s/dws_user_tag_%s " % (user_dir, user_dir, name_suffix))
                if str(old_tag).find("Table has been created") == -1 or str(load_ok).find("FAILED") != -1:
                    MergeLogic.send_email('Reset failed for portrait table!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    raise RuntimeError("➤➤➤➤➤Pay attention to: Reset failed for portrait table!")
        else:
            # 失败
            MergeLogic.update_mysql(all_tag)
            MergeLogic.send_email('Failed to create table or insert data, portrait data is the previous day')
            raise RuntimeError("建表或插入数据失败")

    @staticmethod
    def update_mysql(all_tag):
        conn = Getconf.getdb_con()
        for tag in all_tag:
            update_sql = "update tag_merge set is_delete=1 where tag_name='%s'" % tag
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            cursor.execute(update_sql)
            conn.commit()
        conn.close()

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
        MergeLogic.the_email(content, 'receive_email')

