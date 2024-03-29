# -*- coding:gbk -*-
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
    # 获取随机颜色
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
        print("获取access_token失败，请检查app_id和app_secret是否正确")
        os.system("pause")
        sys.exit(1)
    # print(access_token)
    return access_token


def get_birthday(birthday, year, today):
    birthday_year = birthday.split("-")[0]
    # 判断是否为农历生日
    if birthday_year[0] == "r":
        r_mouth = int(birthday.split("-")[1])
        r_day = int(birthday.split("-")[2])
        # 今年生日
        birthday = ZhDate(year, r_mouth, r_day).to_datetime().date()
        year_date = birthday

    else:
        # 获取国历生日的今年对应月和日
        birthday_month = int(birthday.split("-")[1])
        birthday_day = int(birthday.split("-")[2])
        # 今年生日
        year_date = date(year, birthday_month, birthday_day)
    # 计算生日年份，如果还没过，按当年减，如果过了需要+1
    if today > year_date:
        if birthday_year[0] == "r":
            # 获取农历明年生日的月和日
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
    # 城市id
    try:
        city_id = "101280601"
    except KeyError:
        print("推送消息失败，请检查省份或城市是否正确")
        os.system("pause")
        sys.exit(1)
    # city_id = 101280101
    # 毫秒级时间戳
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
    # 天气
    weather = weatherinfo["weather"]
    # 最高气温
    temp = weatherinfo["temp"]
    # 最低气温
    tempn = weatherinfo["tempn"]
    return weather, temp, tempn


# 词霸每日一句
def get_ciba():
    if (Whether_Eng != "否"):
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


# 彩虹屁
def caihongpi():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # 接口域名
    params = urllib.parse.urlencode({'key': caihongpi_API})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/caihongpi/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    data = data["newslist"][0]["content"]
    if("XXX" in data):
        data.replace("XXX", "花花")
    return data


# 健康小提示API
def health():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # 接口域名
    params = urllib.parse.urlencode({'key': health_API})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/healthtip/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    data = data["newslist"][0]["content"]
    return data


# 星座运势
def lucky():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # 接口域名
    params = urllib.parse.urlencode({'key': lucky_API, 'astro': astro})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/star/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    data = "爱情指数："+str(data["newslist"][1]["content"])+"\n速配星座："+str(data["newslist"][7]["content"])+"\n工作指数："+str(data["newslist"][2]["content"])+"\n今日概述："+str(data["newslist"][8]["content"])
    return data


# 励志名言
def lizhi():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # 接口域名
    params = urllib.parse.urlencode({'key': lizhi_API})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/lzmy/index', params, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    return data["newslist"][0]["saying"]


# 风向和建议
def tip():
    conn = http.client.HTTPSConnection('api.tianapi.com')  # 接口域名
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
    tips = str(tipss).replace("年老体弱者请适当增减衣服。", "")
    return wind, windspeed, sunrise, sunset, tips


# 推送信息
def send_message(to_user, access_token, city_name, weather, max_temperature, min_temperature, pipi, lizhi, wind, windspeed, sunrise, sunset, tips, note_en=None, note_ch=None, health_tip=None, lucky_=None):
    url = "https://api.weixin.qq.com/cgi-bin/message/template/send?access_token={}".format(access_token)
    week_list = ["星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"]
    year = localtime().tm_year
    month = localtime().tm_mon
    day = localtime().tm_mday
    today = datetime.date(datetime(year=year, month=month, day=day))
    week = week_list[today.isoweekday() % 7]
    # 获取在一起的日子的日期格式
    love_year = int("2022-12-05".split("-")[0])
    love_month = int("2022-12-05".split("-")[1])
    love_day = int("2022-12-05".split("-")[2])
    love_date = date(love_year, love_month, love_day)
    # 获取在一起的日期差
    love_days = str(today.__sub__(love_date)).split(" ")[0]
    # 获取所有生日数据
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
        # 获取距离下次生日的时间
        birth_day = get_birthday(value, year, today)
        # 将生日数据插入data
        data["data"][key] = {"value": birth_day, "color": get_color()}
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    response = post(url, headers=headers, json=data).json()
    if response["errcode"] == 40037:
        print("推送消息失败，请检查模板id是否正确")
    elif response["errcode"] == 40036:
        print("推送消息失败，请检查模板id是否为空")
    elif response["errcode"] == 40003:
        print("推送消息失败，请检查微信号是否正确")
    elif response["errcode"] == 0:
        print("推送消息成功")
    else:
        print(response)


if __name__ == "__main__":
    # try:
    #     with open("config.txt", encoding="utf-8") as f:
    #     with open("./girlf_push/config.txt", encoding="utf-8") as f:
    #         config = eval(f.read())
    # except FileNotFoundError:
    #     print("推送消息失败，请检查config.txt文件是否与程序位于同一路径")
    #     os.system("pause")
    #     sys.exit(1)
    # except SyntaxError:
    #     print("推送消息失败，请检查配置文件格式是否正确")
    #     os.system("pause")
    #     sys.exit(1)

    # 获取accessToken
    accessToken = get_access_token()
    # 接收的用户
    users = ["olFD56QC4nnCohgj-27OugQ-Y0CE","olFD56TW2uN20cPag4DUnChiwkgQ"]
    # 传入省份和市获取天气信息
    province, city = "广东", "深圳"
    weather, max_temperature, min_temperature = get_weather(province, city)
    # 获取彩虹屁API
    caihongpi_API = "46e3b6b1bd2d44a1bb1c6cdb49d28cdf"
    # 获取励志古言API
    lizhi_API = "46e3b6b1bd2d44a1bb1c6cdb49d28cdf"
    # 获取天气预报API
    tianqi_API = "46e3b6b1bd2d44a1bb1c6cdb49d28cdf"
    # 是否启用词霸每日金句
    Whether_Eng = "是"
    # 获取健康小提示API
    health_API = ""
    # 获取星座运势API
    lucky_API = ""
    # 获取星座
    astro = ""
    # 获取词霸每日金句
    # note_ch, note_en = get_ciba()
    # 彩虹屁
    pipi = caihongpi()
    # 健康小提示
    # health_tip = health()
    # 下雨概率和建议
    wind, windspeed, sunrise, sunset, tips = tip()
    # 励志名言
    lizhi = lizhi()
    # 星座运势
    # lucky_ = lucky()
    # 公众号推送消息
    for user in users:
        send_message(user, accessToken, city, weather, max_temperature, min_temperature, pipi, lizhi, wind, windspeed, sunrise, sunset, tips)
    import time
    time_duration = 3.5
    time.sleep(time_duration)
