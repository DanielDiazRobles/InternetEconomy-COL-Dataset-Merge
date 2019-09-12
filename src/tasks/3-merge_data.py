import sys
import pandas as pd
import numpy as np
import os
import psycopg2
import logging
from difflib import SequenceMatcher
import configparser
import time

config = configparser.ConfigParser()
config.read("config.ini")

logfilename = 'logs/merge_data_' + time.strftime("%Y%m%d%I%M%S") + '.log'
open(logfilename, 'a').close()
logging.basicConfig(    format='%(levelname)s - %(message)s',
                        filename=logfilename ,
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO
                        )

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

#Consulta  a la BD tabla directorio_clean
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
postgreSQL_select_Query = "SELECT * FROM directorio_clean"
cursor = connection.cursor()
cursor.execute(postgreSQL_select_Query)
directorio = cursor.fetchall()
df_directorio = pd.DataFrame(directorio, columns=['id','TIPO_DOCUMENTO','NIT','DIGITO_VERIFICACION','RAZON_SOCIAL','NOMBRE_COMERCIAL','DIRECCION','MUNI_ID_DPTO','NOMBRE_DPTO','MUNI_ID_MPIO','NOMBRE_MPIO','TELEFONO1','TELEFONO2','WEB','EMAIL','col2','col3'])
logging.info(df_directorio.count())

#Consulta  a la BD tabla dataprovider_clean
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
postgreSQL_select_Query = "SELECT * FROM dataprovider_clean"
cursor = connection.cursor()
cursor.execute(postgreSQL_select_Query)
dataprovider = cursor.fetchall()
df_dataprovider = pd.DataFrame(dataprovider, columns=['id', 'Hostname', 'Continent', 'Country', 'Region', 'Zip code', 'City', 'Address', 'Addresses', 'Company name', 'Company type', 'Company quality', 'Legal entity', 'Business Registry number', 'IBAN number', 'BIC number', 'Tax number', 'Phone number', 'Secondary phone numbers', 'Email address', 'Secondary email addresses', 'Keywords', 'Relevant keywords', 'Subdomain', 'Domain', 'DNS NS domain', 'main_web'])
logging.info(df_dataprovider.count())

#CREANDO COLUMNA DE URL LIMPIA DE DATAPROVIDER
df_dataprovider['Web Page'] = df_dataprovider['Hostname']
df_dataprovider['Hostname'] = df_dataprovider['Hostname'].str.replace('www.','')
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

for index, row in df_dataprovider.iterrows():
    host_dataprovider =  row['Hostname']
    #Copia de dataframe del directorio
    df_csv_direcotrio_copy = df_directorio
    #Filtrado por match
    df_filter = df_csv_direcotrio_copy['WEB'].str.match(host_dataprovider)
    contains_string =  df_filter == True
    #Generando dataframe filtrado
    df_csv__filtered = df_csv_direcotrio_copy[contains_string]
    count = df_csv__filtered['WEB'].count()
    if(count > 0):
        for index2, row2 in df_csv__filtered.iterrows():
            ratio = similar(row['Hostname'],row2['WEB'])
            if ratio > 0.95 :
                merge = {
                    "web_page_dataprovider" : row['Web Page'],
                    "web_page_directorio" : row2['Web Page'],
                    "index_dataprovider" : row['id'],
                    "index_directorio" : row2['id'],
                }
                logging.info(merge)
                result.append(merge)

#        Matrix_result.append(host_dataprovider)
#df_results = pd.DataFrame(Matrix_result, columns = ['Name' ,'results'])


#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla directorio_clean
postgres_delete_query = """ DELETE FROM merge_relations"""
cursor.execute(postgres_delete_query)
connection.commit()

#GUARDANDO INFORMACION EN BD TABLA CLEAN
postgres_insert_query = """ INSERT INTO merge_relations (merge_id,dataprovider_id,directorio_id) VALUES (%s,%s,%s)"""

i = 0
for item in result:
    record_to_insert = (i, item['index_dataprovider'], item['index_directorio'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + " Registros guardados en merge_relations")
result.append(str(i) + " Registros guardados en merge_relations")