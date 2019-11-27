from flask import Blueprint
from bson.json_util import dumps
import pymongo
import os

myclient = pymongo.MongoClient(f'mongodb://192.168.2.2:27017',username="root",password="@H1lcotadmin", unicode_decode_error_handler='ignore')
mydb = myclient["HTERRACOTA"]

METRICAS_API   =   Blueprint('aeropuerto_api',__name__)

def get_blueprint():
    return METRICAS_API

@METRICAS_API.route('/aeropuerto/getdocs/', methods=['GET'])
def get_metricas_clean():
    mycol = mydb["terra_aeropuerto"]
    return dumps(mycol.find()), 200