import warnings

from kafka import KafkaConsumer
import json
from json import dumps
import base64
import ast
from datetime import date, datetime
import datetime
# import h3
from sys import argv
from os.path import exists
# import simplejson as json
from time import mktime
import time
import pandas as pd
# import plotly
# import plotly_express as px
from flask import Flask, render_template, request, flash, jsonify

app = Flask(__name__)
app.secret_key = "kafka-cep-trafik"
app.config['JSON_AS_ASCII'] = False

# access_token = 'pk.eyJ1IjoiYWJkdWxrZXJpbW5lc2UiLCJhIjoiY2s5aThsZWlnMDExcjNkcWFmaWUxcmh3YyJ9.s-4VLvmoPQFPXdu9Mcd6pA'
# px.set_mapbox_access_token(access_token)
warnings.filterwarnings("ignore")

consumer = KafkaConsumer('smartus_anonymous_data', bootstrap_servers='10.5.75.22:9092')
# file = open('smartus16.json', 'w')
res_dict = []


def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


# file write function
# file.write("[")


# column_names = ["udid", "UserPushID", "longitude","latitude","speedValue","appBundleID","os","datetime"]
# df = pd.DataFrame(columns=column_names)

df = pd.DataFrame({'udid': pd.Series(dtype='int'),
                   'UserPushID': pd.Series(dtype='str'),
                   'longitude': pd.Series(dtype='float'),
                   'latitude': pd.Series(dtype='float'),
                   'speedValue': pd.Series(dtype='int'),
                   'appBundleID': pd.Series(dtype='str'),
                   'os': pd.Series(dtype='str'),
                   'datetime': pd.Series(dtype='str')})

# df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

udid_list = []
UserPushID_list = []
longitude_list = []
latitude_list = []
speedValue_list = []
appBundleID_list = []
os_list = []
datetime_list = []
count = 0
for start in range(1, 101):
    for msg in consumer:
        data = msg.value
        times = msg.timestamp
        x = dumps(times, default=myconverter)
        y = datetime.datetime.fromtimestamp(int(x) // 1000)
        z = str(y)
        res = json.loads(data)
        b64_str = res["documentContent"][29:]
        b64_str = b64_str.encode('utf-8')
        b64_bytes = base64.b64decode(b64_str)
        decode_str = b64_bytes.decode('utf-8')
        d = ast.literal_eval(decode_str)
        # print(d)
        udid = d.get('UDID')
        longitude = d.get('longitude')
        speedValue = d.get('speedValue')
        UserPushID = d.get('UserPushID')
        latitude = d.get('latitude')
        appBundleID = d.get('appBundleID')
        # h3code = h3.geo_to_h3(latitude, longitude, 10)
        os = d.get('os')
        udid_list.append(udid)
        UserPushID_list.append(UserPushID)
        longitude_list.append(longitude)
        latitude_list.append(latitude)
        speedValue_list.append(speedValue)
        os_list.append(os)
        datetime_list.append(z)
        appBundleID_list.append(appBundleID)
        # print("m: ", m)
        df = pd.DataFrame({'udid': udid_list, 'UserPushID': UserPushID_list, 'longitude': longitude_list,
                           'latitude': latitude_list,
                           'speedValue': speedValue_list, 'appBundleID': appBundleID_list,
                           'os': os_list, 'datetime': datetime_list})

        df.dropna()
        try:
            df["speedValue"] = abs(df["speedValue"])
        except KeyError:
            pass
        df_p = df.T.to_dict('dict')
        count += 1
        print("count: ", count)
        if count == 10*start:
            break


    @app.route('/api/v1/resources/cep-trafik/all', methods=['GET'])
    def api_all():
        return jsonify(df_p)
    app.run()

# A route to return all of the available entries in our catalog.

# app.run()
# app.run()
#
# app.config['JSON_AS_ASCII'] = False
# app.run()

# fig = px.scatter_mapbox(
#     df, lat="latitude", lon="longitude",
#     size='speedValue',
#     color="udid", color_continuous_scale=px.colors.cyclical.IceFire,
#     hover_name="UserPushID",
#     zoom=8,
#     height=800,
#     width=1200,
#     animation_frame='datetime', animation_group="udid"
# )
# fig.update_layout(mapbox_style="dark", mapbox_accesstoken=access_token)
# fig.layout.coloraxis.showscale = False
# fig.write_html("kafka.html")
# # fig.show()

# @app.route("/hello")
# def index():
#     flash("what's your name?")
#     return render_template("index.html")
#
#
# @app.route("/greet", methods=['POST', 'GET'])
# def greeter():
#     flash("Hi " + str(request.form['name_input']) + ", great to see you!")
#     return render_template("index.html")
