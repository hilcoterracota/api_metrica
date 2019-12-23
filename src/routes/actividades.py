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
            "pestanias":sorted(filter(lambda x: x["nombre"] == app, title_events), key=lambda x: x["tiempoTotal"], reverse=True),
            "mayormenor":usuario_mayor_menor(x)
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
        "pestanias":sorted(filter(lambda x: x["nombre"] in catalogos["info_pc_navegadores"], title_events), key=lambda x: x["tiempoTotal"], reverse=True),
        "mayormenor":usuario_mayor_menor(x)
    })  



    ## APLICATIVOS
    data_aplicativos = []
    for app in catalogos["info_pc_aplicativos"]: 
        x = dataset[dataset["nombre"] == app]
        data_aplicativos.append({
            "nombre":app,
            "tiempoPromedio":sum_time_array_clear_seconds(x["tiempoTotal"].tolist())   ,
            "mayormenor":usuario_mayor_menor(x)
        })

    resumen_aplicaciones.append({
        "titulo":"APLICATIVOS",
        "apps": data_aplicativos,
        "pestanias":sorted(filter(lambda x: x["nombre"] in catalogos["info_pc_aplicativos"], title_events), key=lambda x: x["tiempoTotal"], reverse=True)

    })

    return dumps(resumen_aplicaciones), 200


@ACTIVIDADES_API.route('/metricas/resumenactividadesdiariochart/', methods=['GET'])
def get_metricas_history():
    data = list(myclient["HTERRACOTA"]["ipcht"].find())
    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]

    myclient.close()

    today        = date.today()

    usuarios = pd.DataFrame(list(myclient["HTERRACOTA"]["ipcht"].find()))["host"].tolist()

    app_events = []
    for usuario in data:
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
    data_labels = []
    data_aplicativos = []
    data_ofmatica = []
    data_navegadores = []
    data_otros = []



    for usuario in usuarios:
        result_aplicativos = list(filter(lambda x: x["host"] == usuario.upper() and x["nombre"] in catalogos["info_pc_aplicativos"], app_events))
        result_ofmatica    = list(filter(lambda x: x["host"] == usuario.upper() and x["nombre"] in catalogos["info_pc_office"], app_events))
        result_navegadores = list(filter(lambda x: x["host"] == usuario.upper() and x["nombre"] in catalogos["info_pc_navegadores"], app_events))
        result_otros       = list(filter(lambda x: x["host"] == usuario.upper() and x["nombre"] not in catalogos["info_pc_office"] and x["nombre"] not in catalogos["info_pc_navegadores"]and x["nombre"] not in catalogos["info_pc_aplicativos"]and x["nombre"] not in catalogos["info_pc_exclude"], app_events)) 
        data_labels.append(usuario)
        
        if len(result_aplicativos) == 0:  
            data_aplicativos.append(0)
        else:  
            data_aplicativos.append(  sum_time_array_clear_hour(pd.DataFrame(result_aplicativos )["tiempoTotal"].tolist()))
    
        if len(result_ofmatica) == 0:  
            data_ofmatica.append(0)
        else:   
            data_ofmatica.append(     sum_time_array_clear_hour(pd.DataFrame(result_ofmatica    )["tiempoTotal"].tolist()))
        
        if len(result_navegadores) == 0:
            data_navegadores.append(0)
        else:
            data_navegadores.append(  sum_time_array_clear_hour(pd.DataFrame(result_navegadores )["tiempoTotal"].tolist()))
        
        if len(result_otros) == 0:
            data_otros.append(0)
        else:
            data_otros.append(        sum_time_array_clear_hour(pd.DataFrame(result_otros       )["tiempoTotal"].tolist()))


    response = [{
        "type": "bar",
        "labels": data_labels,
        "data": [
            {
                "data": data_aplicativos,
                "label": "APLICATIVOS"
            },
            {
                "data": data_navegadores,
                "label": "NAVEGADORES"
            },
            {
                "data": data_ofmatica,
                "label": "OFIMATICA"
            },
            {
                "data": data_otros,
                "label": "OTROS"
            }
        ],
        "options": {
            "responsive": True
        }
    }]
    return dumps(response), 200

@ACTIVIDADES_API.route('/metricas/resumenaactividadesdiariochartpie/', methods=['GET'])
def get_metricas_history_pie():
    data = list(myclient["HTERRACOTA"]["ipcht"].find())
    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]

    myclient.close()

    today        = date.today()

    usuarios = pd.DataFrame(list(myclient["HTERRACOTA"]["ipcht"].find()))["host"].tolist()

    app_events = []
    for usuario in data:
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
                

    dataset = pd.DataFrame(app_events)


    ## OFMATICA
    data_ofmatica = []
    for app in catalogos["info_pc_office"]: 
        x = dataset[dataset["nombre"] == app]
        data_ofmatica.append(sum_time_array_clear_hour(x["tiempoTotal"].tolist()))

    #NAVEGADORES
    data_navegadores = []
    for app in catalogos["info_pc_navegadores"]: 
        x = dataset[dataset["nombre"] == app]
        data_navegadores.append(sum_time_array_clear_hour(x["tiempoTotal"].tolist()))

    ## APLICATIVOS
    data_aplicativos = []
    for app in catalogos["info_pc_aplicativos"]: 
        x = dataset[dataset["nombre"] == app]
        data_aplicativos.append(sum_time_array_clear_hour(x["tiempoTotal"].tolist()))
        
    ahora = datetime.now()
    hora_entrada = datetime(ahora.year, ahora.month, ahora.day, hour=9, minute=0)
    horas_laboradas = (ahora - hora_entrada).total_seconds()
    horas_laboradas = round(horas_laboradas /3600,2)


    data_aplicativos_pie = round(sum_time_array_clear_hour_prom(data_aplicativos)/horas_laboradas,2)*10
    data_ofmatica_pie    = round(sum_time_array_clear_hour_prom(data_ofmatica)/horas_laboradas,2)*10
    data_navegadores_pie = round(sum_time_array_clear_hour_prom(data_navegadores)/horas_laboradas,2)*10
    data_sin_uso = round((100-(data_aplicativos_pie+data_ofmatica_pie+data_navegadores_pie)),2)

    response = [{
        "type": "pie",
        "labels": ["OFMATICA","APLICATIVOS","NAVEGADORES","OTROS"],
        "data": [
            {
                "data": [data_ofmatica_pie,data_aplicativos_pie,data_navegadores_pie,data_sin_uso]
            }
        ],
        "options": {
            "responsive": True
        }
    }]
    return dumps(response), 200

@ACTIVIDADES_API.route('/metricas/allnowactividades/', methods=['GET'])
def get_metricas_allnow():
    response = []
    hoy = datetime.now()
    hoy = hoy.strftime("%Y-%m-%d")
    for element in myclient["HTERRACOTA"]["ipcht"].find():

        datos = filter(lambda x: x["timestamp"].split("T")[0] == hoy , element["app_events"]) 

        datos = list(datos)

        response.append({
            "usuario":element["host"],
            "historico":f'http://{element["ip"]}:5600',   
        })
    return dumps(response), 200


def sum_time_array_clear_seconds(entry):
    totalSecs = 0
    for tm in entry:
        totalSecs += tm
    if len(entry)!= 0:
        totalSecs = totalSecs / len(entry)
    totalSecs, sec = divmod(totalSecs, 60)
    hr, min = divmod(totalSecs, 60)
    return "%d:%02d:%02d" % (hr, min, sec)

def usuario_mayor_menor(data):
    minimo = ""
    maximo = ""
    if len(data) != 0:
        data_proc = data.sort_values(by="tiempoTotal")
        minimo = f'{data_proc["host"].tolist()[0]} {sum_time_array_clear_seconds([data_proc["tiempoTotal"].tolist()[0]])}'
        maximo = f'{data_proc["host"].tolist()[len(data_proc)-1]} {sum_time_array_clear_seconds([data_proc["tiempoTotal"].tolist()[len(data_proc)-1]])}'
    return [minimo,maximo]

def sum_time_array_clear_hour(entry):
    totalSecs = 0
    for tm in entry:
        totalSecs += tm
    if len(entry)!= 0:
        totalSecs = totalSecs
    return round(totalSecs/3600,2)

def sum_time_array_clear_hour_prom(entry):
    totalSecs = 0
    for tm in entry:
        totalSecs += tm
    if len(entry)!= 0:
        totalSecs = totalSecs
    return round(totalSecs,2)