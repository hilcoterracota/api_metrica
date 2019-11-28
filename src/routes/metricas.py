from flask import Blueprint
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import pymongo
import os

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')
mydb = myclient["HTERRACOTA"]

METRICAS_API   =   Blueprint('metricas_api',__name__)

def get_blueprint():
    return METRICAS_API

@METRICAS_API.route('/metricas/metricasclean/', methods=['GET'])
def get_metricas_clean():
    mycol = mydb["info_pc_historico"]
    catalogos = list(mydb["catalogos"].find())[0]
    data = []
    for item in mycol.find():
        historico = []
        tiempo_uso_global = datetime.strptime('00:00:00', '%H:%M:%S')
        print(item["usuario"])
        for proseso in item["historico"]:
            if str(proseso["fecha"]) == str(date.today()) and str(proseso["nombre"]) not in catalogos["info_pc_exclude"]:
                historico.append(proseso)          
                h2 = str(proseso["tiempoTotal"]).split(":")
                h2_a = ((int(h2[0]))+int(h2[1])*60)+int(h2[2])
                tiempo_uso_global = tiempo_uso_global + timedelta(seconds=int(h2_a))
        print(str(tiempo_uso_global.strftime("%H:%M:%S")))   
        data.append({
            "usuario":item["usuario"],
            "historico":sorted(historico, key=lambda element: element['usoMemoria'],reverse=True),
            "tiempoTotal":str(tiempo_uso_global.strftime("%H:%M:%S"))
        })

    return dumps(data), 200

@METRICAS_API.route('/metricas/historico/', methods=['GET'])
def get_metricas_history():
    mycol = mydb["info_pc_historico"]
    return dumps(mycol.find()), 200