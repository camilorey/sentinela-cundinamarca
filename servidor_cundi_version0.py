#------------------------------------------------------------------
#Este es el archivo fuente de todos los servicios de datos para cundinamarca
#Esta es la versión al 23 de Noviembre de 2020
#Copiada para el deploy en Heroku
#------------------------------------------------------------------


import flask
from flask import Flask,request,jsonify
import psycopg2 as Psycopg2
import pandas as Pandas

#cuadrando las propiedades de la app
app = Flask(__name__)
PORT = 5000
DEBUG = False
#tablas variables categóricas
tablas_categoricas = {'zona':'zona',
                      'sexo':'sexo',
                      'estado_civil':'estadocivil',
                      'escolaridad':'escolaridad',
                      'delito':'delito'}
                      
tablas_nominales = ['lugar','profesion','marcaVehiculo','color','pais','armaUsada','movil','claseDeEmpleado']
params_orquestador={'hostname':'suleiman.db.elephantsql.com',
                       'username':'bcdcpmha',
                       'database':'bcdcpmha',
                       'password':'fVFa5dcR2zO9-hTJrV8DAj_gKrLKe3Sk'}

nombres_provincias ={'alto_magdalena':'ALTO MAGDALENA',
                     'gualiva':'GUALIVÁ',
                     'tequendama':'TEQUENDAMA',
                     'sumapaz':'SUMAPAZ',
                     'magdalena_centro':'MAGDALENA CENTRO',
                     'sabana_occidente':'SABANA OCCIDENTE',
                     'sabana_centro':'SABANA CENTRO',
                     'bajo_magdalena':'BAJO MAGDALENA',
                     'oriente':'ORIENTE',
                     'ubate':'UBATÉ',
                     'almeidas':'ALMEIDAS',
                     'rionegro':'RIONEGRO',
                     'guavio':'GUAVIO',
                     'medina':'MEDINA',
                     'soacha':'SOACHA'}
#vamos a hacer un método para retornar consultas como Pandas
def queryComoDataFrame(query,db_params):
    conexionDB = None
    resultado = None
    try:
        print("conectando con la base de datos")
        conexionDB = Psycopg2.connect(host=db_params['hostname'],
                                      user=db_params['username'],
                                      password=db_params['password'],
                                      dbname=db_params['database'])
        resultado = Pandas.read_sql_query(query,conexionDB)
        conexionDB.close()
        return resultado
    except (Exception,Psycopg2.DatabaseError) as Error:
        print("hubo un error en la consulta")
        print(Error)
    finally:
        if conexionDB is not None:
            conexionDB.close()
            print("Busqueda ejecutada con éxito, cerrando conexión")

#este método averigua los parámetros de conexión a las provincias
def parametrosConexionTodasLasProvincias():
    query = """SELECT provincia.provincia,
               parametros_bases_provincias.host,
               parametros_bases_provincias.username,
               parametros_bases_provincias.password 
               FROM parametros_bases_provincias JOIN provincia ON provincia.provincia_id = parametros_bases_provincias.provincia_id;"""
    
    info_conexiones = queryComoDataFrame(query,params_orquestador)
    diccionario_provincias = {}
    for idx,fila in info_conexiones.iterrows():
        prov = fila['provincia']
        dic_provincia ={'hostname':fila['host'],
                        'username':fila['username'],
                        'database':fila['username'],
                        'password':fila['password']}
        diccionario_provincias[prov]=dic_provincia
    return diccionario_provincias
        
def parametrosDBProvincia(nomProvincia):
    query = """SELECT host,database,username,password 
               FROM parametros_bases_provincias JOIN provincia ON provincia.provincia_id = parametros_bases_provincias.provincia_id
               WHERE provincia.provincia ='"""+nomProvincia+"';"
    info = queryComoDataFrame(query,params_orquestador)
    host = info.iloc[0]['host']
    db = info.iloc[0]['database']
    user = info.iloc[0]['username']
    password = info.iloc[0]['password']
    return {'hostname':host,
            'username':user,
            'database':db,
            'password':password}
def parametrosDBMunicipio(nomMunicipio):
    query = """ SELECT municipio_provincia.municipio_id,
                       parametros_bases_provincias.host,
                       parametros_bases_provincias.database,
                       parametros_bases_provincias.username,
                       parametros_bases_provincias.password
                       FROM municipio_provincia JOIN parametros_bases_provincias ON municipio_provincia.provincia_id = parametros_bases_provincias.provincia_id
                       WHERE municipio_provincia.nom_clave_municipio='"""+nomMunicipio+"';"
    info = queryComoDataFrame(query,params_orquestador)
    municipio_id = info.iloc[0]['municipio_id']
    host = info.iloc[0]['host']
    db = info.iloc[0]['database']
    user = info.iloc[0]['username']
    password = info.iloc[0]['password']
    return {'hostname':host,
            'username':user,
            'database':db,
            'password':password},str(municipio_id)

#este método permite hacer consultas en las bases usando los nombres
def queryEnProvinciaHTML(query,nomHTML):
    params_db = parametrosDBProvincia(nombres_provincias[nomHTML])
    return queryComoDataFrame(query,params_db)

def queryEnMunicipioHTML(query,nomHTML):
    params_db = parametrosDBMunicipio(nomHTML)
    return queryComoDataFrame(query,params_db)

def get_info_var_categorica(nomVarHTML):
    nom_tabla = tablas_categoricas[nomVarHTML]
    query = "SELECT * FROM "+nom_tabla+" ORDER BY "+nom_tabla+"."+nom_tabla
    info_tabla = queryComoDataFrame(query,params_orquestador)
    return info_tabla

def queryVarCategorica(nomVarHTML):
    if nomVarHTML == 'sexo':
        return """SELECT sexo.sexo,
                         count(incidentePersonal.incidente_id) AS "num_incidentes"
                  FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                                 JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id 
                  GROUP BY sexo.sexo
                  ORDER BY sexo.sexo ASC;"""
    elif nomVarHTML =='estado_civil':
        return """SELECT estadoCivil.estadoCivil,
                         count(incidentePersonal.incidente_id) AS "num_incidentes"
                  FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                                 JOIN estadoCivil ON incidentePersonal.estadoCivil_id = estadoCivil.estadoCivil_id 
                  GROUP BY estadoCivil.estadoCivil
                  ORDER BY estadoCivil.estadoCivil ASC;"""
    elif nomVarHTML =='escolaridad':
        return """SELECT escolaridad.escolaridad,
                  count(incidentePersonal.incidente_id) AS "num_incidentes"
                  FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                                 JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id 
                  GROUP BY escolaridad.escolaridad
                  ORDER BY escolaridad.escolaridad ASC;"""
    elif nomVarHTML =='zona':
        return """SELECT zona.zona,
                         count(incidente.incidente_id) AS "num_incidentes"
                  FROM incidente JOIN zona ON incidente.zona_id = zona.zona_id 
                  GROUP BY zona.zona
                  ORDER BY zona.zona ASC;"""
    else: #nomVarHTML =='delito'
        return """SELECT delito.delito,
                         count(incidente.incidente_id) AS "num_incidentes"
                  FROM incidente JOIN delito ON incidente.delito_id = delito.delito_id 
                  GROUP BY delito.delito
                  ORDER BY delito.delito ASC;"""
    
def parametros_busqueda_por_var_categorica_provincial(nomVarHTML):
    return get_info_var_categorica(nomVarHTML),queryVarCategorica(nomVarHTML)


def cambiar_delito_sexual(nomDelito):
    if 'ART' in nomDelito:
        return 'DELITO SEXUAL'
    elif 'DELITOS SEXUALES' in nomDelito:
        return 'DELITO SEXUAL'
    else:
        return nomDelito
        
#-------------------------------------------------------------------------------
# TABLAS DE CONTINGENCIA POR PROVINCIA
#-------------------------------------------------------------------------------
@app.route('/api/estadisticas/contingencia/delito_vs_zona',methods=['GET'])
def tabla_de_contingencia_delito_vs_zona():
    if 'nom_provincia' in request.args:
        query = """SELECT delito.delito,zona.zona, count(incidente.incidente_id) AS "num_incidentes"
                 FROM incidente JOIN delito ON incidente.delito_id = delito.delito_id
                 JOIN zona ON incidente.zona_id = zona.zona_id
                 GROUP BY delito.delito,zona.zona;"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='zona',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT delito.delito,zona.zona, count(incidente.incidente_id) AS "num_incidentes"
                   FROM incidente JOIN delito ON incidente.delito_id = delito.delito_id
                   JOIN zona ON incidente.zona_id = zona.zona_id
                   WHERE incidente.municipio_id = <muni_id>
                   GROUP BY delito.delito,zona.zona;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='zona',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."

@app.route('/api/estadisticas/contingencia/delito_vs_escolaridad',methods=['GET'])
def tabla_de_contingencia_delito_vs_escolaridad():
    if 'nom_provincia' in request.args:
        query = """SELECT delito.delito,escolaridad.escolaridad, count(incidentePersonal.incidente_id) AS "num_incidentes"
               FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                              JOIN delito ON incidente.delito_id = delito.delito_id
                              JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                              GROUP BY delito.delito,escolaridad.escolaridad;"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='escolaridad',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT delito.delito,escolaridad.escolaridad, count(incidentePersonal.incidente_id) AS "num_incidentes"
                   FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                                  JOIN delito ON incidente.delito_id = delito.delito_id
                                  JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                   WHERE incidente.municipio_id = <muni_id>
                   GROUP BY delito.delito,escolaridad.escolaridad;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='escolaridad',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."

@app.route('/api/estadisticas/contingencia/delito_vs_estado_civil',methods=['GET'])
def tabla_de_contingencia_delito_vs_estado_civil():
    if 'nom_provincia' in request.args:
        query = """SELECT delito.delito,estadocivil.estadocivil, count(incidentePersonal.incidente_id) AS "num_incidentes"
               FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                              JOIN delito ON incidente.delito_id = delito.delito_id
                              JOIN estadocivil ON incidentePersonal.estadocivil_id = estadocivil.estadocivil_id
                              GROUP BY delito.delito,estadocivil.estadocivil;"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='estadocivil',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT delito.delito,estadocivil.estadocivil, count(incidentePersonal.incidente_id) AS "num_incidentes"
               FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                              JOIN delito ON incidente.delito_id = delito.delito_id
                              JOIN estadocivil ON incidentePersonal.estadocivil_id = estadocivil.estadocivil_id
               WHERE incidente.municipio_id = <muni_id>
               GROUP BY delito.delito,estadocivil.estadocivil;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='estadocivil',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."
        
@app.route('/api/estadisticas/contingencia/delito_vs_sexo',methods=['GET'])
def tabla_de_contingencia_delito_vs_sexo():
    if 'nom_provincia' in request.args:
        query = """SELECT delito.delito,sexo.sexo, count(incidentePersonal.incidente_id) AS "num_incidentes"
               FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                              JOIN delito ON incidente.delito_id = delito.delito_id
                              JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
               GROUP BY delito.delito,sexo.sexo;"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT delito.delito,sexo.sexo, count(incidentePersonal.incidente_id) AS "num_incidentes"
                   FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                                  JOIN delito ON incidente.delito_id = delito.delito_id
                                  JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                   WHERE incidente.municipio_id = <muni_id>
                   GROUP BY delito.delito,sexo.sexo;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)
        result['delito'] = result['delito'].apply(lambda x: cambiar_delito_sexual(x)) 
        contingencia = Pandas.pivot_table(data=result,
                                  index='delito',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."

@app.route('/api/estadisticas/contingencia/escolaridad_vs_estado_civil',methods=['GET'])
def tabla_de_contingencia_escolaridad_vs_estado_civil():
    query = """SELECT escolaridad.escolaridad,estadocivil.estadocivil,count(incidentePersonal.incidente_id) AS "num_incidentes"
                 FROM incidentePersonal JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                                        JOIN estadocivil ON incidentePersonal.estadocivil_id = estadocivil.estadocivil_id
                 GROUP BY escolaridad.escolaridad,estadocivil.estadocivil;"""
    if 'nom_provincia' in request.args:
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        contingencia = Pandas.pivot_table(data=result,
                                  index='escolaridad',
                                  columns='estadocivil',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT escolaridad.escolaridad,estadocivil.estadocivil,count(incidentePersonal.incidente_id) AS "num_incidentes"
                 FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id 
                                JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                                JOIN estadocivil ON incidentePersonal.estadocivil_id = estadocivil.estadocivil_id
                 WHERE incidente.municipio_id = <muni_id>
                 GROUP BY escolaridad.escolaridad,estadocivil.estadocivil;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)
        contingencia = Pandas.pivot_table(data=result,
                                  index='escolaridad',
                                  columns='estadocivil',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."

@app.route('/api/estadisticas/contingencia/escolaridad_vs_sexo',methods=['GET'])
def tabla_de_contingencia_escolaridad_vs_sexo():
    query = """SELECT sexo.sexo,escolaridad.escolaridad,count(incidentePersonal.incidente_id) AS "num_incidentes"
                 FROM incidentePersonal JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                                        JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                 GROUP BY sexo.sexo,escolaridad.escolaridad;"""
    if 'nom_provincia' in request.args:
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        contingencia = Pandas.pivot_table(data=result,
                                  index='escolaridad',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT sexo.sexo,escolaridad.escolaridad,count(incidentePersonal.incidente_id) AS "num_incidentes"
                 FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id 
                                JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                                JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                 WHERE incidente.municipio_id = <muni_id>
                 GROUP BY sexo.sexo,escolaridad.escolaridad;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)
        contingencia = Pandas.pivot_table(data=result,
                                  index='escolaridad',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."
        
@app.route('/api/estadisticas/contingencia/estado_civil_vs_sexo',methods=['GET'])
def tabla_de_contingencia_estado_civil_vs_sexo():
    query = """SELECT sexo.sexo,estadocivil.estadocivil,count(incidentePersonal.incidente_id) AS "num_incidentes"
                 FROM incidentePersonal JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                                        JOIN estadocivil ON incidentePersonal.estadocivil_id = estadocivil.estadocivil_id
                 GROUP BY sexo.sexo,estadocivil.estadocivil;"""
    if 'nom_provincia' in request.args:
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        contingencia = Pandas.pivot_table(data=result,
                                  index='estadocivil',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT sexo.sexo,estadocivil.estadocivil,count(incidentePersonal.incidente_id) AS "num_incidentes"
                 FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                                JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                                JOIN estadocivil ON incidentePersonal.estadocivil_id = estadocivil.estadocivil_id
                 WHERE incidente.municipio_id = <muni_id>
                 GROUP BY sexo.sexo,estadocivil.estadocivil;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)                       
        contingencia = Pandas.pivot_table(data=result,
                                  index='estadocivil',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia."

@app.route('/api/estadisticas/contingencia/estado_civil_vs_zona',methods=['GET'])
def tabla_de_contingencia_estado_civil_vs_zona():
    query = """SELECT zona.zona,estadocivil.estadocivil,count(incidentePersonal.incidente_id) AS "num_incidentes"
                FROM incidente JOIN incidentePersonal on incidente.incidente_id = incidentePersonal.incidente_id
                               JOIN estadoCivil ON incidentePersonal.estadocivil_id = estadoCivil.estadoCivil_id
                               JOIN zona ON incidente.zona_id = zona.zona_id
                               GROUP BY zona.zona,estadocivil.estadocivil;"""
    if 'nom_provincia' in request.args:
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        contingencia = Pandas.pivot_table(data=result,
                                  index='estadocivil',
                                  columns='zona',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT zona.zona,estadocivil.estadocivil,count(incidentePersonal.incidente_id) AS "num_incidentes"
                FROM incidente JOIN incidentePersonal on incidente.incidente_id = incidentePersonal.incidente_id
                               JOIN estadoCivil ON incidentePersonal.estadocivil_id = estadoCivil.estadoCivil_id
                               JOIN zona ON incidente.zona_id = zona.zona_id
                               WHERE municipio_id = <muni_id>
                               GROUP BY zona.zona,estadocivil.estadocivil;""".replace('<muni_id>',muni_id)
        result = queryComoDataFrame(query,params_db_muni)                       
        contingencia = Pandas.pivot_table(data=result,
                                  index='estadocivil',
                                  columns='zona',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')                      
    else:
        return "Error. No se especificó nombre de la provincia."
        
@app.route('/api/estadisticas/contingencia/zona_vs_sexo',methods=['GET'])
def tabla_de_contingencia_zona_vs_sexo():
    query = """SELECT zona.zona,sexo.sexo,count(incidentePersonal.incidente_id) AS "num_incidentes"
                FROM incidente JOIN incidentePersonal on incidente.incidente_id = incidentePersonal.incidente_id
                               JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                               JOIN zona ON incidente.zona_id = zona.zona_id
                               GROUP BY zona.zona,sexo.sexo;"""
    if 'nom_provincia' in request.args:
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        contingencia = Pandas.pivot_table(data=result,
                                  index='zona',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT zona.zona,sexo.sexo,count(incidentePersonal.incidente_id) AS "num_incidentes"
                FROM incidente JOIN incidentePersonal on incidente.incidente_id = incidentePersonal.incidente_id
                               JOIN sexo ON incidentePersonal.sexo_id = sexo.sexo_id
                               JOIN zona ON incidente.zona_id = zona.zona_id
                               WHERE incidente.municipio_id = <muni_id> 
                               GROUP BY zona.zona,sexo.sexo;""".replace('<muni_id>',muni_id)                       
        result = queryComoDataFrame(query,params_db_muni)
        contingencia = Pandas.pivot_table(data=result,
                                  index='zona',
                                  columns='sexo',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
        
    else:
        return "Error. No se especificó nombre de la provincia."

@app.route('/api/estadisticas/contingencia/escolaridad_vs_zona',methods=['GET'])
def tabla_de_contingencia_escolaridad_vs_zona():
    if 'nom_provincia' in request.args:
        query = """SELECT zona.zona,escolaridad.escolaridad,count(incidente.incidente_id) AS "num_incidentes"
               FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                              JOIN zona ON incidente.zona_id = zona.zona_id
                              JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
               GROUP BY zona.zona,escolaridad.escolaridad"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        contingencia = Pandas.pivot_table(data=result,
                                  index='escolaridad',
                                  columns='zona',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    elif 'nom_municipio' in request.args:
        nom_municipio = request.args['nom_municipio']
        params_db_muni,muni_id = parametrosDBMunicipio(nom_municipio)
        query = """SELECT zona.zona,escolaridad.escolaridad,count(incidente.incidente_id) AS "num_incidentes"
               FROM incidente JOIN incidentePersonal ON incidente.incidente_id = incidentePersonal.incidente_id
                              JOIN zona ON incidente.zona_id = zona.zona_id
                              JOIN escolaridad ON incidentePersonal.escolaridad_id = escolaridad.escolaridad_id
                              WHERE incidente.municipio_id = """+muni_id+'\n'+\
                           """GROUP BY zona.zona,escolaridad.escolaridad"""
        result = queryComoDataFrame(query,params_db_muni)
        contingencia = Pandas.pivot_table(data=result,
                                  index='escolaridad',
                                  columns='zona',
                                  values='num_incidentes',
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
        return contingencia.to_json(orient='records')
    else:
        return "Error. No se especificó nombre de la provincia, ni nombre de municipio."
        
#---------------------------------------------------------------------------
# REPORTES
#---------------------------------------------------------------------------

@app.route('/api/reportes/provincia/var_categorica',methods=['GET'])
def reporte_delitos_por_var():
    if 'var_categorica' not in request.args:
        return "Error. no hay variable categórica en el request"
    else:
        diccionario_conexiones = parametrosConexionTodasLasProvincias()
        var_categorica = request.args['var_categorica']
        nom_original = tablas_categoricas[var_categorica]
        info_categorica,query=parametros_busqueda_por_var_categorica_provincial(var_categorica)
        categorias = list(info_categorica[nom_original])
        categorias_de_no = []
        categorias_sexuales = []
        for categ in categorias:
            if ('ART' in categ) or ('SEX' in categ):
                categorias_sexuales.append(categ)
        data_provincias = Pandas.DataFrame(columns=['provincia']+categorias)
        for provHTML in nombres_provincias.keys():
            fila = {'provincia':nombres_provincias[provHTML]}
            result_prov = queryComoDataFrame(query,diccionario_conexiones[nombres_provincias[provHTML]])
            for cat in categorias:
                fila[cat] = 0
                sub_data = result_prov.loc[result_prov[nom_original]==cat]
                if len(sub_data) !=0: 
                    fila[cat] = sub_data.iloc[0]['num_incidentes']
            data_provincias = data_provincias.append(fila,ignore_index=True)
        #retornamos este nuevo dataframe con la info de las provincias 
        #vamos a fusionar los delitos sexuales en una sola
        if len(categorias_sexuales)!=0:
            data_provincias['DELITO SEXUAL'] = data_provincias.loc[:,categorias_sexuales].sum(axis=1)
            data_provincias = data_provincias.drop(columns=categorias_sexuales)
            #datos_sin_ceros = datos_sin_ceros.drop(columns = categorias_sexuales)
        #retornamos los resultados     
        return data_provincias.to_json(orient='records')

@app.route('/api/reportes/municipio/incidentes_de_municipio',methods=['GET'])
def incidentes_de_municipio():
    if 'nom_municipio' in request.args:
        nomMunicipio = request.args['nom_municipio']
        params_muni_db,muni_id = parametrosDBMunicipio(nomMunicipio)
        query = """SELECT delito.delito, 
                          count(incidente.incidente_id) AS "num_incidentes"
                    FROM incidente JOIN delito ON incidente.delito_id = delito.delito_id
                    WHERE incidente.municipio_id ="""+ muni_id +\
                    """GROUP BY delito.delito
                    ORDER BY num_incidentes DESC;"""
        #buscamos en la base de datos del municipio
        result = queryComoDataFrame(query,params_muni_db)
        return result.to_json(orient='records')
    else:
        return "Error: nombre de municipio no provisto"
        
@app.route('/api/reportes/provincia/incidentes_municipios',methods=['GET'])
def incidentes_municipios_provincia():
    if 'nom_provincia' in request.args:
        query = """SELECT municipio.municipio, count(incidente.incidente_id) AS "num_incidentes"
                   FROM incidente JOIN municipio ON incidente.municipio_id = municipio.municipio_id
                   GROUP BY municipio.municipio 
                   ORDER BY municipio.municipio ASC;"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        return result.to_json(orient='records')
    else:
        return "Error: no se especificó nombre de provincia"

@app.route('/api/reportes/incidentes_provincias',methods=['GET'])
def incidentes_provincias():
    if 'nom_provincia' in request.args:
        query = """SELECT delito.delito,
                      count(incidente.incidente_id) AS "num_incidentes" 
               FROM incidente JOIN delito ON incidente.delito_id = delito.delito_id
               GROUP BY delito.delito
               ORDER BY num_incidentes DESC;"""
        nom_prov = request.args['nom_provincia']
        result = queryEnProvinciaHTML(query,nom_prov)
        return result.to_json(orient='records')
    else:
        return "Error: no se especificó nombre de provincia"

#---------------------------------------------------------------------------
# BÚSQUEDAS DE DELITOS 
#---------------------------------------------------------------------------

@app.route('/api/busquedas/incidencias_delito_cada_municipio',methods=['GET'])
def incidencias_delito_cada_municipio():
    if 'delito_search' in request.args:
        delito_param = request.args['delito_search'].upper()
        query = """SELECT municipio.municipio, 
                          count(incidente.incidente_id) as "num_incidentes"
                   FROM incidente JOIN municipio ON incidente.municipio_id = municipio.municipio_id
                                  JOIN delito ON incidente.delito_id = delito.delito_id
                   WHERE delito.delito LIKE '%"""+delito_param+"""%'
                   GROUP BY municipio.municipio
                   ORDER BY municipio.municipio DESC;"""
        #vamos a hacer una búsqueda en todas las bases de datos individuales           
        result_provs = []
        for provHTML in nombres_provincias.keys():
            result_prov = queryEnProvinciaHTML(query,provHTML)
            result_provs.append(result_prov)
        #concatenamos el resultado en un solo dataframe
        result_general = Pandas.concat(result_provs)
        return result_general.to_json(orient='records')
    else:
        return "Error. No se especificó un delito para buscar"



#---------------------------------------------------------------------------
# RAIZ DE LA API 
#---------------------------------------------------------------------------
@app.route('/',methods=['GET'])
def index():
    return """<h1> Servicios Web para consultar datos de Seguridad Cundinamarca </h1>
               <p> Aquí desarrollamos una serie de servicios web que permiten consultar los datos
                   de SIEDCO para el departamento de Cundinamarca</p>
               <p> Desarrollado por: Camilo Rey Torres (La Mapera 2020)"""

if __name__ =='__main__':
    app.run(port=PORT,debug=DEBUG)