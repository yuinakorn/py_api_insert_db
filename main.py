import json
import time

import requests
import pymysql.cursors

# pip install python-dotenv
from dotenv import dotenv_values

config_env = dotenv_values(".env")

connection = pymysql.connect(host=config_env["HOST"],
                             user=config_env["USER_DB"],
                             password=config_env["PASS_DB"],
                             db=config_env["DB_NAME"],
                             charset=config_env["DB_CHARSET"],
                             )

api_url = config_env["API_URL"]

# get json data from hoscode.json
with open('hoscode.json') as f:
    data = json.load(f)
    # json array to list
    for i in data:
        hoscode_list = i['hos_code']
        print(hoscode_list)

with open('params.json') as f:
    data = json.load(f)
    # json array to list
    for i in data:
        params_list = i['params']
        print(params_list)

# hoscode = '11138'
# table_name = 'hosxp_school'

for i in params_list:
    table_name = i
    for j in hoscode_list:
        hoscode = j

        url = api_url + "/" + table_name + "/" + hoscode
        print(url)

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        status = response.status_code

        json_arr = response.text
        json_obj = json.loads(json_arr)

        for item in json_obj:
            dictionary = item

            headers = []
            values = []
            for key in dictionary:
                value = dictionary[key]
                headers.append(key)
                values.append(value)

            # convert list to string with double quote
            headers_str = ','.join(headers)
            values_str = '"' + '","'.join(map(str, values)) + '"'  # map(str, values) convert int to str
            # replace "None" to NULL
            values_str = values_str.replace('\"None\"', 'NULL')

            sql = "INSERT INTO " + table_name + " (" + headers_str + ") VALUES (" + values_str + ");"
            # print(sql)

            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    connection.commit()  # commit the changes
            except Exception as e:
                print(e)
                connection.rollback()  # rollback if any exception occurred (optional)

        time.sleep(3)  # sleep 3 seconds

connection.close()
