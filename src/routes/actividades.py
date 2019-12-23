from flask import Blueprint, request
import pymongo
import uuid
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import time
import os
import calendar
import pandas as pd

myclient = pymongo.MongoClient(f'mongodb://{os.environ["MONGO_URL"]}:27017',username=os.environ["MONGO_USER"],password=os.environ["MONGO_PS"], unicode_decode_error_handler='ignore',connect = False  )

ACTIVIDADES_API   =   Blueprint('actividades_api',__name__)

def get_blueprint():
    return ACTIVIDADES_API

@ACTIVIDADES_API.route('/metricas/resumenactividadesdiario/', methods=['GET'])
def get_metricas_clean():
    usuarios = list(myclient["HTERRACOTA"]["ipcht"].find())

    mydb = myclient["HTERRACOTA"]
    usuarios = list(myclient["HTERRACOTA"]["ipcht"].find())

    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]

    myclient.close()

    today        = date.today()
    app_events   = []
    title_events = []
    for usuario in usuarios:
        for event in usuario["app_events"]:
            fecha = event ["timestamp"].split("T")
            fecha = fecha[0]
            if str(fecha) == str(today):
                app_events.append({
                    "host":usuario["host"].upper(),
                    "timestamp":fecha,
                    "tiempoTotal":event["duration"],
                    "nombre":event["data"]["app"].upper().replace(".EXE","")

                })
            
        for event in usuario["title_events"]:
            fecha = event ["timestamp"].split("T")
            fecha = fecha[0]
            if str(fecha) == str(today):
                title_events.append({
                    "host":usuario["host"].upper(),
                    "timestamp":fecha,
                    "tiempoTotal":event["duration"],
                    "nombre":event["data"]["app"].upper().replace(".EXE",""),
                    "title":event["data"]["title"]

                })

    dataset = pd.DataFrame(app_events)
    # RESUMEN
    resumen_aplicaciones = []

    ## OFMATICA
    data_ofmatica = []

    for app in catalogos["info_pc_office"]: 
        x = dataset[dataset["nombre"] == app]
        data_ofmatica.append({
            "nombre":app,
            "tiempoPromedio":sum_time_array_clear_seconds(x["tiempoTotal"].tolist()),
            "pestanias":sorted(filter(lambda x: x["nombre"] == app, title_events), key=lambda x: x["tiempoTotal"], reverse=True)

        })
    resumen_aplicaciones.append({
        "titulo":"OFMATICA",
        "apps": data_ofmatica,
        "pestanias":sorted(filter(lambda x: x["nombre"] in catalogos["info_pc_office"], title_events), key=lambda x: x["tiempoTotal"], reverse=True)
    })

    ## NAVEGADORES
    data_navegadores = []
    for app in catalogos["info_pc_navegadores"]: 
        x = dataset[dataset["nombre"] == app]
        for y in x["tiempoTotal"].tolist():
            data_navegadores.append(y)
            
    resumen_aplicaciones.append({
        "nombre":"NAVEGADORES",
        "tiempoPromedio":sum_time_array_clear_seconds(data_navegadores),
        "pestanias":sorted(filter(lambda x: x["nombre"] in catalogos["info_pc_navegadores"], title_events), key=lambda x: x["tiempoTotal"], reverse=True)

    })  



    ## APLICATIVOS
    data_aplicativos = []
    for app in catalogos["info_pc_aplicativos"]: 
        x = dataset[dataset["nombre"] == app]
        data_aplicativos.append({
            "nombre":app,
            "tiempoPromedio":sum_time_array_clear_seconds(x["tiempoTotal"].tolist())    
        })

    resumen_aplicaciones.append({
        "titulo":"APLICATIVOS",
        "apps": data_aplicativos,
        "pestanias":sorted(filter(lambda x: x["nombre"] in catalogos["info_pc_aplicativos"], title_events), key=lambda x: x["tiempoTotal"], reverse=True)

    })


    return dumps(resumen_aplicaciones), 200




def sum_time_array_clear_seconds(entry):
    totalSecs = 0
    for tm in entry:
        totalSecs += tm
    if len(entry)!= 0:
        totalSecs = totalSecs / len(entry)
    totalSecs, sec = divmod(totalSecs, 60)
    hr, min = divmod(totalSecs, 60)
    return "%d:%02d:%02d" % (hr, min, sec)