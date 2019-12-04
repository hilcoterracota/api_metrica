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
            "tiempoPromedio":sum_time_array(x["tiempoTotal"].tolist(),True)
        })
    for app in catalogos["info_pc_navegadores"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy] 
        for y in x["tiempoTotal"].tolist():
            t_navegadores.append(y)
    data_ofmatica.append({
        "nombre":"NAVEGADORES",
        "tiempoPromedio":sum_time_array(t_navegadores,True)
    })  

    resumen_aplicaciones.append({
        "titulo":"ofmatica",
        "tTotal": sum_time_array(pd.DataFrame(data_ofmatica)["tiempoPromedio"],False),
        "apps": data_ofmatica
    })

    ## APLICATIVOS
    data_aplicativos = []
    for app in catalogos["info_pc_aplicativos"]: 
        x = dataset[dataset["nombre"] == app]
        x = x[dataset["fecha"] == hoy]
        data_aplicativos.append({
            "nombre":app,
            "tiempoPromedio":sum_time_array(x["tiempoTotal"].tolist(),True)
        })
        
    resumen_aplicaciones.append({
        "titulo":"ofmatica",
        "tTotal": sum_time_array(pd.DataFrame(data_aplicativos)["tiempoPromedio"],False),
        "apps": data_aplicativos
    })

    return dumps(resumen_aplicaciones), 200

@METRICAS_API.route('/metricas/historico/', methods=['GET'])
def get_metricas_history():
    mycol = myclient["HTERRACOTA"]["info_pc_historico"]
    return dumps(mycol.find()), 200

@METRICAS_API.route('/metricas/personalizado/', methods=['POST'])
def get_metricas_personalizado():
    rqst = request.json
    d_inicio = datetime.strptime(rqst["finicio"], '%m/%d/%Y')
    d_final = datetime.strptime(rqst["ffinal"], '%m/%d/%Y')
    if d_inicio > d_final:
        d_final = datetime.strptime(rqst["finicio"], '%m/%d/%Y')
        d_inicio = datetime.strptime(rqst["ffinal"], '%m/%d/%Y')

    snapshot = []
    for usuario in myclient["HTERRACOTA"]["info_pc_historico"].find():
        if usuario["usuario"] in rqst["usuarios"] or len(rqst["usuarios"]) == 0:
            snapshot.append({
                "usuario":usuario["usuario"],
                "historico": list(filter(lambda x: 
                    datetime.strptime(x["fecha"], '%Y-%m-%d') >= d_inicio and 
                    datetime.strptime(x["fecha"], '%Y-%m-%d') <= d_final, 
                    usuario["historico"]))
            })
                        
    catalogos = list(myclient["HTERRACOTA"]["catalogos"].find())[0]

    data = []
    resumen_global = {}

    tiempo_resumes_global_navegadores = datetime.strptime('00:00:00', '%H:%M:%S')
    tiempo_resumes_global_otros = datetime.strptime('00:00:00', '%H:%M:%S')
    tiempo_resumes_global_office = {}

    for item in snapshot:
        historico = []
        tiempo_uso_global = datetime.strptime('00:00:00', '%H:%M:%S')
        for proseso in item["historico"]:
            if str(proseso["nombre"]) not in catalogos["info_pc_exclude"]:
                historico.append(proseso)          
                h2 = str(proseso["tiempoTotal"]).split(":")
                h2_a = ((int(h2[0])*3600)+int(h2[1])*60)+int(h2[2])
                tiempo_uso_global = tiempo_uso_global + timedelta(seconds=int(h2_a))

            if str(proseso["nombre"]) in catalogos["info_pc_navegadores"]:
                tugn_ = str(proseso["tiempoTotal"]).split(":")
                tugn = ((int(tugn_[0])*3600)+int(tugn_[1])*60)+int(tugn_[2])
                tiempo_resumes_global_navegadores = tiempo_resumes_global_navegadores + timedelta(seconds=int(tugn))

            if str(proseso["nombre"]) in catalogos["info_pc_office"]:
                tugn_ = str(proseso["tiempoTotal"]).split(":")
                tugn = ((int(tugn_[0])*3600)+int(tugn_[1])*60)+int(tugn_[2])
                if str(proseso["nombre"]) in tiempo_resumes_global_office:
                    tiempo_resumes_global_office[str(proseso["nombre"])] = tiempo_resumes_global_office[str(proseso["nombre"])] + timedelta(seconds=int(tugn))
                else:
                    trgf = datetime.strptime('00:00:00', '%H:%M:%S')
                    tiempo_resumes_global_office[str(proseso["nombre"])]= trgf + timedelta(seconds=int(tugn))

            if str(proseso["nombre"]) not in catalogos["info_pc_office"] and str(proseso["nombre"]) not in catalogos["info_pc_navegadores"]:
                tugn_ = str(proseso["tiempoTotal"]).split(":")
                tugn = ((int(tugn_[0])*3600)+int(tugn_[1])*60)+int(tugn_[2])
                tiempo_resumes_global_otros = tiempo_resumes_global_otros + timedelta(seconds=int(tugn))

        data.append({
            "usuario":item["usuario"],
            "historico":sorted(historico, key=lambda element: element['usoMemoria'],reverse=True),
            "tiempoTotal":str(tiempo_uso_global.strftime("%H:%M:%S"))
        })

    x = str(round((d_final-d_inicio).days/7,2)).split(".")

    hora_aux = (int(x[0])*5)
    if hora_aux != 0:
        n2 = (float(str(f'.{x[1]}'))*5)
        print
        if n2 >= 1:
            hora_aux = hora_aux + int(str(n2).split(".")[0])
    else:
        hora_aux = 1


    tugn_aux = str(tiempo_resumes_global_navegadores.strftime("%H:%M:%S")).split(":")
    tugnn = ((int(tugn_aux[0])*3600)+int(tugn_aux[1])*60)+int(tugn_aux[2])
    tugnnt = (tugnn/len(snapshot)) 
    resumen_global["pNavegadores"] = round((tugnnt*100)/(hora_aux*32400),2)


    tugn_aux = str(tiempo_resumes_global_otros.strftime("%H:%M:%S")).split(":")
    tugnn = ((int(tugn_aux[0])*3600)+int(tugn_aux[1])*60)+int(tugn_aux[2])
    tugnnt = (tugnn/len(snapshot)) 
    resumen_global["pOtros"] = round((tugnnt*100)/(hora_aux*32400),2)

    for catalogo in catalogos["info_pc_office"]:
        if catalogo in tiempo_resumes_global_office:
            tugn_aux = str(tiempo_resumes_global_office[catalogo].strftime("%H:%M:%S")).split(":")
            tugnn = ((int(tugn_aux[0])*3600)+int(tugn_aux[1])*60)+int(tugn_aux[2])
            tugnnt = (tugnn/len(snapshot)) 
            resumen_global["p"+catalogo]= round((tugnnt*100)/(hora_aux*32400),2)
        else:
            resumen_global["p"+catalogo]= 0

    data_now = {
        "users": data,
        "charts": []
        
    }

    data_now["charts"].append({
        "type": "radar",
        "labels": ["NAVEGADORES", "WINWORD", "EXCEL", "ONEDRIVE", "TEAMS", "OUTLOOK", "POWERPNT", "OTROS"],
        "data": [
            {
                "data": [resumen_global["pNavegadores"], resumen_global["pWINWORD"], resumen_global["pEXCEL"], resumen_global["pONEDRIVE"], resumen_global["pTEAMS"], resumen_global["pOUTLOOK"], resumen_global["pPOWERPNT"],resumen_global["pOtros"]]
            }
        ],
        "options": {
            "responsive": True,
            "animation": {
                "duration": 0
            }
        }
    })

    total = round(resumen_global["pNavegadores"]+ resumen_global["pWINWORD"]+ resumen_global["pEXCEL"]+ resumen_global["pONEDRIVE"]+ resumen_global["pTEAMS"]+ resumen_global["pOUTLOOK"]+ resumen_global["pPOWERPNT"]+resumen_global["pOtros"])
    print(total)

    data_now["charts"].append({
        "type": "pie",
        "labels": ["EN USO", "ESTATICA"],
        "data": [
            {
                "data": [total,(100-total)]
            }
        ],
        "options": {
            "responsive": True,
            "animation": {
                "duration": 0
            }
        }
    })

    return dumps(data_now), 200

def sum_time_array(entry,promedio):
    t = datetime.strptime('00:00:00', '%H:%M:%S')
    for item in entry:
        if promedio:
            p = len(entry)
        else:
            p = 1
        h, m, s = item.split(':')
        t = t + timedelta(hours=int(h)/p, minutes=int(m)/p, seconds=int(s)/p)
    return t.strftime("%H:%M:%S")