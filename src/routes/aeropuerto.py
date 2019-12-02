from flask import Blueprint
from bson.json_util import dumps
import pymongo
import os

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')
mydb = myclient["HTERRACOTA"]

METRICAS_API   =   Blueprint('aeropuerto_api',__name__)

def get_blueprint():
    return METRICAS_API

@METRICAS_API.route('/aeropuerto/getdocs/', methods=['GET'])
def get_getdocs():
    mycol = mydb["terra_aeropuerto"]
    return dumps(mycol.find()), 200