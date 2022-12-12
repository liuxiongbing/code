#!/usr/bin/env python
# -*- coding:utf-8 -*-
import random
from time import time, localtime
import cityinfo
from requests import get, post
from datetime import datetime, date
import sys
import os
import http.client, urllib
import json
from zhdate import ZhDate


def get_color():
    # ��ȡ�����ɫ
    get_colors = lambda n: list(map(lambda i: "#" + "%06x" % random.randint(0, 0xFFFFFF), range(n)))
    color_list = get_colors(100)
    return random.choice(color_list)


def get_access_token():
    # appId
    app_id = "wx3d267c9b1077e434"
    # appSecret
    app_secret = "4e90a14e064f6087017ff3a2ca37c4e5"
    post_url = ("https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={}&secret={}"
                .format(app_id, app_secret))
    try:
        access_token = get(post_url).json()['access_token']
    except KeyError:
        print("��ȡaccess_tokenʧ�ܣ�����app_id��app_secret�Ƿ���ȷ")
        os.system("pause")
        sys.exit(1)
    # print(access_token)
    return access_token


def get_birthday(birthday, year, today):
    birthday_year = birthday.split("-")[0]
    # �ж��Ƿ�Ϊũ������
    if birthday_year[0] == "r":
        r_mouth = int(birthday.split("-")[1])
        r_day = int(birthday.split("-")[2])
        # ��������
        birthday = ZhDate(year, r_mouth, r_day).to_datetime().date()
        year_date = birthday

    else:
        # ��ȡ�������յĽ����Ӧ�º���
        birthday_month = int(birthday.split("-")[1])
        birthday_day = int(birthday.split("-")[2])
        # ��������
        year_date = date(year, birthday_month, birthday_day)
    # ����������ݣ������û����������������������Ҫ+1
    if today > year_date:
        if birthday_year[0] == "r":
            # ��ȡũ���������յ��º���
            r_last_birthday = ZhDate((year + 1), r_mouth, r_day).to_datetime().date()
            birth_date = date((year + 1), r_last_birthday.month, r_last_birthday.day)
        else:
            birth_date = date((year + 1), birthday_month, birthday_day)
        birth_day = str(birth_date.__sub__(today)).split(" ")[0]
    elif today == year_date:
        birth_day = 0
    else:
        birth_date = year_date
        birth_day = str(birth_date.__sub__(today)).split(" ")[0]
    return birth_day


def get_weather(province, city):
    # ����id
    try:
        city_id = "101280601"
    except KeyError:
        print("������Ϣʧ�ܣ�����ʡ�ݻ�����Ƿ���ȷ")
        os.system("pause")
        sys.exit(1)
    # city_id = 101280101
    # ���뼶ʱ���
    t = (int(round(time() * 1000)))
    headers = {
        "Referer": "http://www.weather.com.cn/weather1d/{}.shtml".format(city_id),
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    url = "http://d1.weather.com.cn/dingzhi/{}.html?_={}".format(city_id, t)
    response = get(url, headers=headers)
    response.encoding = "utf-8"
    response_data = response.text.split(";")[0].split("=")[-1]
    response_json = eval(response_data)
    # print(response_json)
    weatherinfo = response_json["weatherinfo"]
    # ����
    weather = weatherinfo["weather"]
    # �������
    temp = weatherinfo["temp"]
    # �������
    tempn = weatherinfo["tempn"]
    return weather, temp, tempn


# �ʰ�ÿ��һ��
def get_ciba():
    if (Whether_Eng != "��"):
        url = "http://open.iciba.com/dsapi/"
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        }
        r = get(url, headers=headers)
        note_en = r.json()["content"]
        note_ch = r.json()["note"]
        return note_ch, note_en
    else:
        return "", ""


# �ʺ�ƨ
def caihongpi():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # �ӿ�����
    params = urllib.parse.urlencode({'key': caihongpi_API})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/caihongpi/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    data = data["newslist"][0]["content"]
    if("XXX" in data):
        data.replace("XXX", "����")
    return data


# ����С��ʾAPI
def health():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # �ӿ�����
    params = urllib.parse.urlencode({'key': health_API})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/healthtip/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    data = data["newslist"][0]["content"]
    return data


# ��������
def lucky():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # �ӿ�����
    params = urllib.parse.urlencode({'key': lucky_API, 'astro': astro})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/star/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    data = "����ָ����"+str(data["newslist"][1]["content"])+"\n����������"+str(data["newslist"][7]["content"])+"\n����ָ����"+str(data["newslist"][2]["content"])+"\n���ո�����"+str(data["newslist"][8]["content"])
    return data


# ��־����
def lizhi():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # �ӿ�����
    params = urllib.parse.urlencode({'key': lizhi_API})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/lzmy/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    return data["newslist"][0]["saying"]


# ����ͽ���
def tip():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # �ӿ�����
    params = urllib.parse.urlencode({'key': tianqi_API, 'city': city})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/tianqi/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    print(data)
    wind = data["newslist"][0]["wind"]
    windspeed = data["newslist"][0]["windspeed"]
    sunrise = data["newslist"][0]["sunrise"]
    sunset = data["newslist"][0]["sunset"]
    tipss = data["newslist"][0]["tips"]
    tips = str(tipss).replace("�������������ʵ������·���", "")
    return wind, windspeed, sunrise, sunset, tips


# ������Ϣ
def send_message(to_user, access_token, city_name, weather, max_temperature, min_temperature, pipi, lizhi, wind, windspeed, sunrise, sunset, tips, note_en=None, note_ch=None, health_tip=None, lucky_=None):
    url = "https://api.weixin.qq.com/cgi-bin/message/template/send?access_token={}".format(access_token)
    week_list = ["������", "����һ", "���ڶ�", "������", "������", "������", "������"]
    year = localtime().tm_year
    month = localtime().tm_mon
    day = localtime().tm_mday
    today = datetime.date(datetime(year=year, month=month, day=day))
    week = week_list[today.isoweekday() % 7]
    # ��ȡ��һ������ӵ����ڸ�ʽ
    love_year = int("2022-12-05".split("-")[0])
    love_month = int("2022-12-05".split("-")[1])
    love_day = int("2022-12-05".split("-")[2])
    love_date = date(love_year, love_month, love_day)
    # ��ȡ��һ������ڲ�
    love_days = str(today.__sub__(love_date)).split(" ")[0]
    # ��ȡ������������
    birthdays = {"birthday1": "r1996-09-10"}
    # for k, v in config.items():
    #     if k[0:5] == "birth":
    #         birthdays[k] = v
    data = {
        "touser": to_user,
        "template_id": "clTz8su7MqHvManKkr_CZKlO01foynt_YLVg5x2gwR8",
        "url": "http://weixin.qq.com/download",
        "topcolor": "#FF0000",
        "data": {
            "date": {
                "value": "{} {}".format(today, week),
                "color": get_color()
            },
            "city": {
                "value": city_name,
                "color": get_color()
            },
            "weather": {
                "value": weather,
                "color": get_color()
            },
            "min_temperature": {
                "value": min_temperature,
                "color": get_color()
            },
            "max_temperature": {
                "value": max_temperature,
                "color": get_color()
            },
            "love_day": {
                "value": love_days,
                "color": get_color()
            },
            "note_en": {
                "value": note_en,
                "color": get_color()
            },
            "note_ch": {
                "value": note_ch,
                "color": get_color()
            },

            "pipi": {
                "value": pipi,
                "color": get_color()
            },

            "lucky": {
                "value": lucky_,
                "color": get_color()
            },

            "lizhi": {
                "value": lizhi,
                "color": get_color()
            },

            "wind": {
                "value": wind,
                "color": get_color()
            },

            "windspeed": {
                "value": windspeed,
                "color": get_color()
            },

            "sunrise": {
                "value": sunrise,
                "color": get_color()
            },

            "sunset": {
                "value": sunset,
                "color": get_color()
            },

            "health": {
                "value": health_tip,
                "color": get_color()
            },

            "tips": {
                "value": tips,
                "color": get_color()
            }
        }
    }
    for key, value in birthdays.items():
        # ��ȡ�����´����յ�ʱ��
        birth_day = get_birthday(value, year, today)
        # ���������ݲ���data
        data["data"][key] = {"value": birth_day, "color": get_color()}
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    response = post(url, headers=headers, json=data).json()
    if response["errcode"] == 40037:
        print("������Ϣʧ�ܣ�����ģ��id�Ƿ���ȷ")
    elif response["errcode"] == 40036:
        print("������Ϣʧ�ܣ�����ģ��id�Ƿ�Ϊ��")
    elif response["errcode"] == 40003:
        print("������Ϣʧ�ܣ�����΢�ź��Ƿ���ȷ")
    elif response["errcode"] == 0:
        print("������Ϣ�ɹ�")
    else:
        print(response)


if __name__ == "__main__":
    # try:
    #     with open("config.txt", encoding="utf-8") as f:
    #     with open("./girlf_push/config.txt", encoding="utf-8") as f:
    #         config = eval(f.read())
    # except FileNotFoundError:
    #     print("������Ϣʧ�ܣ�����config.txt�ļ��Ƿ������λ��ͬһ·��")
    #     os.system("pause")
    #     sys.exit(1)
    # except SyntaxError:
    #     print("������Ϣʧ�ܣ����������ļ���ʽ�Ƿ���ȷ")
    #     os.system("pause")
    #     sys.exit(1)

    # ��ȡaccessToken
    accessToken = get_access_token()
    # ���յ��û�
    users = ["olFD56QC4nnCohgj-27OugQ-Y0CE"]
    # ����ʡ�ݺ��л�ȡ������Ϣ
    province, city = "�㶫", "����"
    weather, max_temperature, min_temperature = get_weather(province, city)
    # ��ȡ�ʺ�ƨAPI
    caihongpi_API = "46e3b6b1bd2d44a1bb1c6cdb49d28cdf"
    # ��ȡ��־����API
    lizhi_API = "46e3b6b1bd2d44a1bb1c6cdb49d28cdf"
    # ��ȡ����Ԥ��API
    tianqi_API = "46e3b6b1bd2d44a1bb1c6cdb49d28cdf"
    # �Ƿ����ôʰ�ÿ�ս��
    Whether_Eng = "��"
    # ��ȡ����С��ʾAPI
    health_API = ""
    # ��ȡ��������API
    lucky_API = ""
    # ��ȡ����
    astro = ""
    # ��ȡ�ʰ�ÿ�ս��
    # note_ch, note_en = get_ciba()
    # �ʺ�ƨ
    pipi = caihongpi()
    # ����С��ʾ
    # health_tip = health()
    # ������ʺͽ���
    wind, windspeed, sunrise, sunset, tips = tip()
    # ��־����
    lizhi = lizhi()
    # ��������
    # lucky_ = lucky()
    # ���ں�������Ϣ
    for user in users:
        send_message(user, accessToken, city, weather, max_temperature, min_temperature, pipi, lizhi, wind, windspeed, sunrise, sunset, tips)
    import time
    time_duration = 3.5
    time.sleep(time_duration)
