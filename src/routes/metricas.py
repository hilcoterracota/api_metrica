from flask import Blueprint
from bson.json_util import dumps
import pymongo
import os

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"])
mydb = myclient["HTERRACOTA"]

METRICAS_API   =   Blueprint('metricas_api',__name__)

def get_blueprint():
    return METRICAS_API

@METRICAS_API.route('/metricasclean/', methods=['GET'])
def get_metricas_clean():
    mycol = mydb["info_pc_clear"]
    return dumps(mycol.find()), 200