import sys
import pandas as pd
import numpy as np
import os
import psycopg2
import logging
import Levenshtein as lev
from difflib import SequenceMatcher
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)



logging.basicConfig(filename='logs/merge_data.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

#Consulta  a la BD tabla directorio_clean
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
postgreSQL_select_Query = "SELECT * FROM directorio_clean"
cursor = connection.cursor()
cursor.execute(postgreSQL_select_Query)
directorio = cursor.fetchall()
df_directorio = pd.DataFrame(directorio, columns=['id','TIPO_DOCUMENTO','NIT','DIGITO_VERIFICACION','RAZON_SOCIAL','NOMBRE_COMERCIAL','DIRECCION','MUNI_ID_DPTO','NOMBRE_DPTO','MUNI_ID_MPIO','NOMBRE_MPIO','TELEFONO1','TELEFONO2','WEB','EMAIL','col2','col3'])
logging.warning(df_directorio.count())

#Consulta  a la BD tabla dataprovider_clean
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
postgreSQL_select_Query = "SELECT * FROM dataprovider_clean"
cursor = connection.cursor()
cursor.execute(postgreSQL_select_Query)
dataprovider = cursor.fetchall()
df_dataprovider = pd.DataFrame(dataprovider, columns=['id', 'Hostname', 'Continent', 'Country', 'Region', 'Zip code', 'City', 'Address', 'Addresses', 'Company name', 'Company type', 'Company quality', 'Legal entity', 'Business Registry number', 'IBAN number', 'BIC number', 'Tax number', 'Phone number', 'Secondary phone numbers', 'Email address', 'Secondary email addresses', 'Keywords', 'Relevant keywords', 'Subdomain', 'Domain', 'DNS NS domain', 'main_web'])
logging.warning(df_dataprovider.count())

df_directorio = df_directorio.apply(lambda x: x.str.lower() if x.dtype == "object" else x)

#CREANDO COLUMNA DE URL LIMPIA DE DATAPROVIDER
df_dataprovider['Web Page'] = df_dataprovider['Hostname']
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('www.','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.com','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.co','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.gov','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.edu','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.org','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.net','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.io','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.ve','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.us','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.es','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.me','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.in','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.cl','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.pe','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.mx','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.we','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.uk','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.eu','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.xyz','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.ong','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.direct','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.book','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.info','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.site','')
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('.blog','')

#CREANDO COLUMNA DE URL LIMPIA DE DIRECTORIO
df_directorio['Web Page'] = df_directorio['WEB']
df_directorio['WEB'] = df_directorio['WEB'].str.replace('www.','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.com','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.co','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.gov','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.edu','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.org','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.net','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.io','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.ve','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.us','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.es','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.me','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.in','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.cl','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.pe','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.mx','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.we','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.uk','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.eu','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.xyz','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.ong','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.direct','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.book','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.info','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.site','')
df_directorio['WEB'] = df_directorio['WEB'].str.replace('.blog','')

result = []


#FILTRANDO POR SITIOS UNICAMENTE CON WEB
for i in df_directorio.index:
    host_direcotrio = df_directorio.get_value(i,'WEB')
    if host_direcotrio == "":
        df_directorio.set_value(i,'WEB',np.nan)

df_directorio_no_na_web = df_directorio.dropna(subset=['WEB'])
print(df_directorio_no_na_web.count())


for i in df_dataprovider.index:
    try :
        host_dataprovider = df_dataprovider.get_value(i,'Hostname')
        id_dataprovider = df_dataprovider.get_value(i,'id')
        web_page_dataprovider = df_dataprovider.get_value(i,'Web Page')
        df_csv_filtered = df_directorio_no_na_web[df_directorio_no_na_web['WEB'].str.contains(host_dataprovider)]
        count = df_csv_filtered['WEB'].count()
        if(count > 0):
            for j in df_csv_filtered.index:
                host_directorio = df_csv_filtered.get_value(j,'WEB')
                id_directorio = df_csv_filtered.get_value(j,'id')
                web_page_directorio = df_csv_filtered.get_value(j,'Web Page')

                Ratio = lev.ratio(host_dataprovider.lower(),host_directorio.lower())
                if Ratio > 0.80:
                    merge = {
                        "web_page_dataprovider" : host_dataprovider,
                        "web_page_directorio" : host_directorio,
                        "index_dataprovider" : id_dataprovider,
                        "index_directorio" : id_directorio,
                        "ratio" : Ratio,
                        "item" : "Hostname",
                        "value_comparation_dataprovider" : web_page_dataprovider,
                        "value_comparation_directorio" : web_page_directorio
                    }
                    print(merge)
                    logging.warning(merge)
                    result.append(merge)
    except :
        print("error de indice")


df_directorio = df_directorio.apply(lambda x: x.str.lower() if x.dtype == "object" else x)


#FILTRANDO POR REGISTROS CON NOMBRE DE COMPAÑIA
for i in df_directorio.index:
    host_direcotrio = df_directorio.get_value(i,'RAZON_SOCIAL')
    if host_direcotrio == "":
        df_directorio.set_value(i,'RAZON_SOCIAL',np.nan)

df_directorio_no_na_razon = df_directorio.dropna(subset=['RAZON_SOCIAL'])
print(df_directorio_no_na_razon.count())


for i in df_dataprovider.index:
    host_direcotrio = df_dataprovider.get_value(i,'Company name')
    if host_direcotrio == "":
        df_dataprovider.set_value(i,'Company name',np.nan)

df_dataprovider_no_na_name = df_dataprovider.dropna(subset=['Company name'])
print(df_dataprovider_no_na_name.count())



#MERGE POR NOMBRE COMERCIAL
for i in df_dataprovider_no_na_name.index:
    try :
        name_dataprovider = df_dataprovider_no_na_name.get_value(i,'Company name')
        id_dataprovider = df_dataprovider_no_na_name.get_value(i,'id')
        web_page_dataprovider = df_dataprovider_no_na_name.get_value(i,'Web Page')
        df_csv_filtered = df_directorio_no_na_razon[df_directorio_no_na_razon['NOMBRE_COMERCIAL'].str.match(name_dataprovider)]
        count = df_csv_filtered['NOMBRE_COMERCIAL'].count()
        if(count > 0):
            for j in df_csv_filtered.index:
                name_directorio = df_csv_filtered.get_value(j,'NOMBRE_COMERCIAL')
                id_directorio = df_csv_filtered.get_value(j,'id')
                web_page_directorio = df_csv_filtered.get_value(j,'Web Page')

                Ratio = lev.ratio(name_dataprovider.lower(),name_directorio.lower())
                if Ratio > 0.70:
                    merge = {
                        "web_page_dataprovider" : web_page_dataprovider,
                        "web_page_directorio" : web_page_directorio,
                        "index_dataprovider" : id_dataprovider,
                        "index_directorio" : id_directorio,
                        "ratio" : Ratio,
                        "item" : "Nombre Comercial",
                        "value_comparation_dataprovider" : name_dataprovider,
                        "value_comparation_directorio" : name_directorio
                    }
                    print(merge)
                    logging.warning(merge)
                    result.append(merge)
    except :
        print("error de indice")




#MERGE POR RAZON SOCIAL
for i in df_dataprovider_no_na_name.index:
    try :
        name_dataprovider = df_dataprovider_no_na_name.get_value(i,'Company name')
        id_dataprovider = df_dataprovider_no_na_name.get_value(i,'id')
        web_page_dataprovider = df_dataprovider_no_na_name.get_value(i,'Web Page')
        df_csv_filtered = df_directorio_no_na_razon[df_directorio_no_na_razon['RAZON_SOCIAL'].str.match(name_dataprovider)]
        count = df_csv_filtered['RAZON_SOCIAL'].count()
        if(count > 0):
            for j in df_csv_filtered.index:
                name_directorio = df_csv_filtered.get_value(j,'RAZON_SOCIAL')
                id_directorio = df_csv_filtered.get_value(j,'id')
                web_page_directorio = df_csv_filtered.get_value(j,'Web Page')

                Ratio = lev.ratio(name_dataprovider.lower(),name_directorio.lower())
                if Ratio > 0.70:
                    merge = {
                        "web_page_dataprovider" : web_page_dataprovider,
                        "web_page_directorio" : web_page_directorio,
                        "index_dataprovider" : id_dataprovider,
                        "index_directorio" : id_directorio,
                        "ratio" : Ratio,
                        "item" : "Razón Social",
                        "value_comparation_dataprovider" : name_dataprovider,
                        "value_comparation_directorio" : name_directorio
                    }
                    print(merge)
                    logging.warning(merge)
                    result.append(merge)
    except :
        print("error de indice")







#FILTRANDO POR REGISTROS EMAIL
for i in df_directorio.index:
    email_direcotrio = df_directorio.get_value(i,'EMAIL')
    if email_direcotrio == "":
        df_directorio.set_value(i,'EMAIL',np.nan)

df_directorio_no_na_email = df_directorio.dropna(subset=['EMAIL'])
print(df_directorio_no_na_email.count())


for i in df_dataprovider.index:
    email_dataprovider = df_dataprovider.get_value(i,'Email address')
    if email_dataprovider == "":
        df_dataprovider.set_value(i,'Email address',np.nan)

df_dataprovider_no_na_email = df_dataprovider.dropna(subset=['Email address'])
print(df_dataprovider_no_na_email.count())


#MERGE POR EMAIL
for i in df_dataprovider_no_na_email.index:
    try:
        email_dataprovider = df_dataprovider_no_na_email.get_value(i,'Email address')
        id_dataprovider = df_dataprovider_no_na_email.get_value(i,'id')
        web_page_dataprovider = df_dataprovider_no_na_email.get_value(i,'Web Page')
        df_csv_filtered = df_directorio_no_na_email[df_directorio_no_na_email['EMAIL'].str.contains(email_dataprovider)]
        count = df_csv_filtered['EMAIL'].count()
        if(count > 0):

            for j in df_csv_filtered.index:
                email_directorio = df_csv_filtered.get_value(j,'EMAIL')
                id_directorio = df_csv_filtered.get_value(j,'id')
                web_page_directorio = df_csv_filtered.get_value(j,'Web Page')

                Ratio = lev.ratio(email_dataprovider.lower(),email_directorio.lower())
                if Ratio > 0.90:
                    merge = {
                        "web_page_dataprovider" : web_page_dataprovider,
                        "web_page_directorio" : web_page_directorio,
                        "index_dataprovider" : id_dataprovider,
                        "index_directorio" : id_directorio,
                        "ratio" : Ratio,
                        "item" : "Email",
                        "value_comparation_dataprovider" : email_dataprovider,
                        "value_comparation_directorio" : email_directorio
                    }
                    print(merge)
                    logging.warning(merge)
                    result.append(merge)
    except :
        print("error de indice")
print(len(result))





#FILTRANDO POR REGISTROS EMAIL
for i in df_dataprovider.index:
    email_dataprovider = df_dataprovider.get_value(i,'Secondary email addresses')
    if email_dataprovider == "":
        df_dataprovider.set_value(i,'Secondary email addresses',np.nan)

df_dataprovider_no_na_email = df_dataprovider.dropna(subset=['Secondary email addresses'])
print(df_dataprovider_no_na_email.count())


#MERGE POR EMAIL
for i in df_dataprovider_no_na_email.index:
    try:
        email_dataprovider = df_dataprovider_no_na_email.get_value(i,'Secondary email addresses')
        id_dataprovider = df_dataprovider_no_na_email.get_value(i,'id')
        web_page_dataprovider = df_dataprovider_no_na_email.get_value(i,'Web Page')
        df_csv_filtered = df_directorio_no_na_email[df_directorio_no_na_email['EMAIL'].str.contains(email_dataprovider)]
        count = df_csv_filtered['EMAIL'].count()
        if(count > 0):

            for j in df_csv_filtered.index:
                email_directorio = df_csv_filtered.get_value(j,'EMAIL')
                id_directorio = df_csv_filtered.get_value(j,'id')
                web_page_directorio = df_csv_filtered.get_value(j,'Web Page')

                Ratio = lev.ratio(email_dataprovider.lower(),email_directorio.lower())
                if Ratio > 0.90:
                    merge = {
                        "web_page_dataprovider" : web_page_dataprovider,
                        "web_page_directorio" : web_page_directorio,
                        "index_dataprovider" : id_dataprovider,
                        "index_directorio" : id_directorio,
                        "ratio" : Ratio,
                        "item" : "Secondary Email",
                        "value_comparation_dataprovider" : email_dataprovider,
                        "value_comparation_directorio" : email_directorio
                    }
                    print(merge)
                    logging.warning(merge)
                    result.append(merge)
    except :
        print("error de indice")
print(len(result))






#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()


#Limpiando la tabla directorio_clean
postgres_delete_query = """ DELETE FROM merge_relations"""
cursor.execute(postgres_delete_query)
connection.commit()


#GUARDANDO INFORMACION EN BD TABLA CLEAN
postgres_insert_query = """ INSERT INTO merge_relations (merge_id,dataprovider_id,directorio_id,web_page_dataprovider,web_page_directorio,value_comparation_dataprovider,value_comparation_directorio,ratio,item) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

i = 0
for item in result:
    record_to_insert = (i, item['index_dataprovider'], item['index_directorio'], item['web_page_dataprovider'], item['web_page_directorio'], item['value_comparation_dataprovider'], item['value_comparation_directorio'],item['ratio'],item['item'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + " Registros guardados en merge_relations")
result.append(str(i) + " Registros guardados en merge_relations")
