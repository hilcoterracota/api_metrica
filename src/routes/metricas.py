from flask import Blueprint
from bson.json_util import dumps
from datetime import datetime, timedelta
import pymongo
import os

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore')
mydb = myclient["HTERRACOTA"]

METRICAS_API   =   Blueprint('metricas_api',__name__)

def get_blueprint():
    return METRICAS_API

@METRICAS_API.route('/metricas/metricasclean/', methods=['GET'])
def get_metricas_clean():
    mycol = mydb["info_pc"]
    snapshot= []
    for usuario in mycol.find():
        userId = str(usuario['hostiduiid'])
        listaprosesos = []
        listaprosesos_aux  = []
        nombre_usuario = ""
        tiempo_uso_global = datetime.strptime('00:00:00', '%H:%M:%S')
        for proseso in usuario['infoprosses']: 
            lista_pestanias = []
            kb_uso_memoria = 0
            tiempo_uso_app = datetime.strptime('00:00:00', '%H:%M:%S')
            for pestania in usuario['infoprosses']:
                if str(proseso['nombredeimagen']) == str(pestania['nombredeimagen']):
                    if str(pestania["tiempodecpu"]) != "0:00:00" and "HILCOTERRACOTA" in str(pestania["nombredeusuario"]):
                        if "Unknown" != proseso['estado']:
                            print(proseso['estado'])
                        lista_pestanias.append({
                            "tituloVentana": str(pestania["ttulodeventana"]),
                            "tiempoDeUso": str(pestania["tiempodecpu"])
                        })
                        time_aux = str(pestania["tiempodecpu"]).split(":")
                        minutos = (int(time_aux[0])*60)+int(time_aux[1])
                        seconds_aux = (minutos*60)+int(time_aux[2])
                        ##FACTOR TIMEMPO USO/CPU
                        tiempo_uso_app = tiempo_uso_app + timedelta(seconds=int(seconds_aux)*5)
                        tiempo_uso_global = tiempo_uso_global + timedelta(econds=int(seconds_aux)*5)
                        nombre_usuario =  str(pestania["nombredeusuario"])     
                    kb_uso_memoria = kb_uso_memoria + float(str(pestania["usodememoria"]).replace("N/D", "0").replace(",", "").replace(" ", "").replace("KB", "")) 

            if str(proseso['nombredeimagen']) not in listaprosesos:   
                listaprosesos.append(proseso['nombredeimagen'])
                listaprosesos_aux.append({
                    "nombre":proseso['nombredeimagen'].replace(".exe", "").replace(".EXE", "").upper(),
                    "usoMemoria": kb_uso_memoria * 0.001,
                    "tiempoTotal": str(tiempo_uso_app.strftime("%H:%M:%S")),
                    "estado":proseso['estado'],
                    "ventanas":lista_pestanias
                })     
        au = filter(lambda x: x["tiempoTotal"].split(":")[1] != "00", listaprosesos_aux)
        if(nombre_usuario != ""):
            nombre_usuario = nombre_usuario.split("\\")[1]
        snapshot.append({
            "userId":userId,
            "usuario":nombre_usuario,
            "listaprosesos":sorted(list(au), key=lambda element: element['usoMemoria'],reverse=True),
            "tiempoUsoGlobal": str(tiempo_uso_global.strftime("%H:%M:%S"))
        })
    return dumps(snapshot), 200