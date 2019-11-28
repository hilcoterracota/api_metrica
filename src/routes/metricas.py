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
    resumen_global = {}

    tiempo_resumes_global_navegadores = datetime.strptime('00:00:00', '%H:%M:%S')
    tiempo_resumes_global_otros = datetime.strptime('00:00:00', '%H:%M:%S')
    tiempo_resumes_global_office = {}

    for item in mycol.find():
        historico = []
        tiempo_uso_global = datetime.strptime('00:00:00', '%H:%M:%S')
        for proseso in item["historico"]:
            if str(proseso["fecha"]) == str(date.today()) and str(proseso["nombre"]) not in catalogos["info_pc_exclude"]:
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

    hoy = datetime.now()
    hora_entrada = datetime(hoy.year, hoy.month, hoy.day, hour=9, minute=0)

    tugn_aux = str(tiempo_resumes_global_navegadores.strftime("%H:%M:%S")).split(":")
    tugnn = ((int(tugn_aux[0])*3600)+int(tugn_aux[1])*60)+int(tugn_aux[2])
    tugnnt = (tugnn/len(list(mycol.find()))) 
    resumen_global["pNavegadores"] = round((tugnnt*100)/(datetime.now()-hora_entrada).total_seconds(),2)


    tugn_aux = str(tiempo_resumes_global_otros.strftime("%H:%M:%S")).split(":")
    tugnn = ((int(tugn_aux[0])*3600)+int(tugn_aux[1])*60)+int(tugn_aux[2])
    tugnnt = (tugnn/len(list(mycol.find()))) 
    resumen_global["pOtros"] = round((tugnnt*100)/(datetime.now()-hora_entrada).total_seconds(),2)

    for catalogo in catalogos["info_pc_office"]:
        if catalogo in tiempo_resumes_global_office:
            tugn_aux = str(tiempo_resumes_global_office[catalogo].strftime("%H:%M:%S")).split(":")
            tugnn = ((int(tugn_aux[0])*3600)+int(tugn_aux[1])*60)+int(tugn_aux[2])
            tugnnt = (tugnn/len(list(mycol.find()))) 
            resumen_global["p"+catalogo]= round((tugnnt*100)/(datetime.now()-hora_entrada).total_seconds(),2)
    
    data_now = {
        "users": data,
        "chart": {
            "type": "radar",
            "labels": ["NAVEGADORES", "WINWORD", "EXCEL", "ONEDRIVE", "TEAMS", "OUTLOOK", "POWERPNT", "OTROS"],
            "data": [
                {
                    "data": [resumen_global["pNavegadores"], resumen_global["pWINWORD"], resumen_global["pEXCEL"], resumen_global["pONEDRIVE"], resumen_global["pTEAMS"], resumen_global["pOUTLOOK"], resumen_global["pPOWERPNT"],resumen_global["pOtros"]],
                    "label": str(str((datetime.now()-hora_entrada)) + str(" EN USO"))
                }
            ],
            "options": {
                "responsive": True,
                "animation": {
                    "duration": 0
                }
            }
        }
    }

    return dumps(data_now), 200

@METRICAS_API.route('/metricas/historico/', methods=['GET'])
def get_metricas_history():
    mycol = mydb["info_pc_historico"]
    return dumps(mycol.find()), 200