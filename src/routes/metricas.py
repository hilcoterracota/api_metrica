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

METRICAS_API   =   Blueprint('metricas_api',__name__)

def get_blueprint():
    return METRICAS_API

@METRICAS_API.route('/metricas/resumenaplicativosdiario/', methods=['GET'])
def get_metricas_clean():
    #resumenaplicativosdiario
    hoy = datetime.now()
    hoy = hoy.strftime("%Y-%m-%d")

    data = [] 
    for element in myclient["HTERRACOTA"]["info_pc_historico"].find():
        for proseso in element["historico"]:
            data.append({
                "usuario":element["usuario"],
                "nombre":proseso["nombre"],
                "tiempoTotal":proseso["tiempoTotal"],
                "fecha":proseso["fecha"],
            })
    dataset = pd.DataFrame(data)

    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]

    # RESUMEN
    resumen_aplicaciones = []

    ## OFMATICA
    data_ofmatica = []
    t_navegadores = []
    for app in catalogos["info_pc_office"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy]
        data_ofmatica.append({
            "nombre":app,
            "tiempoPromedio":sum_time_array_promedio(x["tiempoTotal"].tolist())
        })
    for app in catalogos["info_pc_navegadores"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy] 
        for y in x["tiempoTotal"].tolist():
            t_navegadores.append(y)
    data_ofmatica.append({
        "nombre":"NAVEGADORES",
        "tiempoPromedio":sum_time_array_promedio(t_navegadores)
    })  

    resumen_aplicaciones.append({
        "titulo":"OFMATICA",
        "tTotal": sum_time_array_promedio(pd.DataFrame(data_ofmatica)["tiempoPromedio"]),
        "apps": data_ofmatica
    })

    ## APLICATIVOS
    data_aplicativos = []
    for app in catalogos["info_pc_aplicativos"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy]
        data_aplicativos.append({
            "nombre":app,
            "tiempoPromedio":sum_time_array_clear(x["tiempoTotal"].tolist())
        })

    resumen_aplicaciones.append({
        "titulo":"APLICATIVOS",
        "tTotal": sum_time_array_clear(pd.DataFrame(data_aplicativos)["tiempoPromedio"]),
        "apps": data_aplicativos
    })

    return dumps(resumen_aplicaciones), 200

@METRICAS_API.route('/metricas/resumenaplicativosdiariochart/', methods=['GET'])
def get_metricas_history():
    #resumenaplicativosdiariochart
    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]
    hoy = datetime.now()
    hoy = hoy.strftime("%Y-%m-%d")

    data = [] 
    for element in myclient["HTERRACOTA"]["info_pc_historico"].find():
        for proseso in element["historico"]:
            data.append({
                "usuario":element["usuario"],
                "nombre":proseso["nombre"],
                "tiempoTotal":proseso["tiempoTotal"],
                "fecha":proseso["fecha"],
            })


    usuarios = pd.DataFrame(list(myclient["HTERRACOTA"]["info_pc_historico"].find()))["usuario"].tolist()
    data = list(filter(lambda x: x["fecha"] == hoy , data))

    data_labels = []
    data_aplicativos = []
    data_ofmatica = []
    data_navegadores = []
    data_otros = []

    for usuario in usuarios:
        result_aplicativos = list(filter(lambda x: x["usuario"] == usuario and x["nombre"] in catalogos["info_pc_aplicativos"], data))
        result_ofmatica    = list(filter(lambda x: x["usuario"] == usuario and x["nombre"] in catalogos["info_pc_office"], data))
        result_navegadores = list(filter(lambda x: x["usuario"] == usuario and x["nombre"] in catalogos["info_pc_navegadores"], data))
        result_otros       = list(filter(lambda x: x["usuario"] == usuario and x["nombre"] not in catalogos["info_pc_office"] and x["nombre"] not in catalogos["info_pc_navegadores"]and x["nombre"] not in catalogos["info_pc_aplicativos"]and x["nombre"] not in catalogos["info_pc_exclude"], data)) 
        data_labels.append(usuario)

        
        if len(result_aplicativos) == 0:  
            data_aplicativos.append(0)
        else:  
            data_aplicativos.append(  sum_time_array_hours(pd.DataFrame(result_aplicativos )["tiempoTotal"].tolist()))
    
        if len(result_ofmatica) == 0:  
            data_ofmatica.append(0)
        else:   
            data_ofmatica.append(     sum_time_array_hours(pd.DataFrame(result_ofmatica    )["tiempoTotal"].tolist()))
        
        if len(result_navegadores) == 0:
            data_navegadores.append(0)
        else:
            data_navegadores.append(  sum_time_array_hours(pd.DataFrame(result_navegadores )["tiempoTotal"].tolist()))
        
        if len(result_otros) == 0:
            data_otros.append(0)
        else:
            data_otros.append(        sum_time_array_hours(pd.DataFrame(result_otros       )["tiempoTotal"].tolist()))

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
                "label": "OFMATICA"
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

@METRICAS_API.route('/metricas/resumenaplicativosdiariochartpie/', methods=['GET'])
def get_metricas_history_pie():
    #resumenaplicativosdiariochartpie
    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]
    hoy = datetime.now()
    hoy = hoy.strftime("%Y-%m-%d")

    data = [] 
    for element in myclient["HTERRACOTA"]["info_pc_historico"].find():
        for proseso in element["historico"]:
            data.append({
                "usuario":element["usuario"],
                "nombre":proseso["nombre"],
                "tiempoTotal":proseso["tiempoTotal"],
                "fecha":proseso["fecha"],
            })

    dataset = pd.DataFrame(data)
    catalogos = myclient["HTERRACOTA"]["catalogos"].find()
    catalogos = catalogos[0]

    ## OFMATICA
    data_ofmatica = []
    for app in catalogos["info_pc_office"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy]
        data_ofmatica.append(sum_time_array_clear(x["tiempoTotal"].tolist()))

    #NAVEGADORES
    data_navegadores = []
    for app in catalogos["info_pc_navegadores"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy] 
        data_navegadores.append(sum_time_array_clear(x["tiempoTotal"].tolist()))

    ## APLICATIVOS
    data_aplicativos = []
    for app in catalogos["info_pc_aplicativos"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy]
        data_aplicativos.append(sum_time_array_clear(x["tiempoTotal"].tolist()))


    ahora = datetime.now()
    hora_entrada = datetime(ahora.year, ahora.month, ahora.day, hour=9, minute=0)
    horas_laboradas = (ahora - hora_entrada).total_seconds()
    horas_laboradas = horas_laboradas/3600

    data_aplicativos_pie = round(sum_time_array_hours(data_aplicativos )*10,2)
    data_ofmatica_pie    = round(sum_time_array_hours(data_ofmatica )*10,2)
    data_navegadores_pie = round(sum_time_array_hours(data_navegadores )*10,2)

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

@METRICAS_API.route('/metricas/allnow/', methods=['GET'])
def get_metricas_allnow():
    response = []
    hoy = datetime.now()
    hoy = hoy.strftime("%Y-%m-%d")
    for element in myclient["HTERRACOTA"]["info_pc_historico"].find():
        datos = filter(lambda x: x["fecha"] == hoy , element["historico"]) 
        datos = list(datos)
        if len(datos) != 0:
            response.append({
                "tTotal":sum_time_array_clear(pd.DataFrame(datos)["tiempoTotal"]),
                "usuario":element["usuario"],
                "historico":datos,   
            })
        else :
            response.append({
                "tTotal":'00:00:00',
                "usuario":element["usuario"],
                "historico":datos,   
            })
            
    return dumps(response), 200

@METRICAS_API.route('/metricas/getindicadores/', methods=['GET'])
def get_metricas_indicadores():
    response = myclient["HTERRACOTA"]["info_terra_ing"].find()
    return dumps(list(response)), 200


def sum_time_array_promedio(entry):
    ahora           = datetime.now()
    totalSecs       = 0
    cat_horario     = myclient["HTERRACOTA"]["catalogos"].find()[0]["horario"]
    factor_tiempo_cpu = myclient["HTERRACOTA"]["catalogos"].find()[0]["factor_tiempo_cpu"]
    h_inicio        = [int(s) for s in cat_horario["h_inicio"].split(':')]
    hora_entrada    = datetime(ahora.year, ahora.month, ahora.day, hour=h_inicio[0], minute=h_inicio[1])
    for tm in entry:
        timeParts = [int(s) for s in tm.split(':')]
        totalSecs += (timeParts[0] * 60 + timeParts[1]) * 60 + timeParts[2]  
    totalSecs, sec = divmod(totalSecs, 60)
    hr, min        = divmod(totalSecs, 60)
    horas_de_uso   = hora_entrada + timedelta(hours=hr, minutes=min ,seconds=sec) 
    R1 = (horas_de_uso-hora_entrada)
    if R1.total_seconds() <= 0:
        return "0:00:00"
    else:
        resultado      = ((horas_de_uso-hora_entrada)*factor_tiempo_cpu)/len(entry)
    return str(resultado).split('.')[0]

def sum_time_array_clear(entry):
    totalSecs = 0
    for tm in entry:
        timeParts = [int(s) for s in tm.split(':')]
        totalSecs += (timeParts[0] * 60 + timeParts[1]) * 60 + timeParts[2]
    totalSecs, sec = divmod(totalSecs, 60)
    hr, min = divmod(totalSecs, 60)
    return "%d:%02d:%02d" % (hr, min, sec)

def sum_time_array_hours(entry):
    totalSecs = 0
    timeParts = [int(s) for s in sum_time_array_promedio(entry).split(':')]
    totalSecs += (timeParts[0] * 60 + timeParts[1]) * 60 + timeParts[2]
    return round(totalSecs/3600,2)
