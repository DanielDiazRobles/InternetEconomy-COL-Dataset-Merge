import pandas as pd
import numpy as np
import os
#import psycopg2
from difflib import SequenceMatcher
import logging
import psycopg2
import sys
import configparser
import time

config = configparser.ConfigParser()
config.read("config.ini")

logfilename = 'logs/clean_directorio_' + time.strftime("%Y%m%d%I%M%S") + '.log'
open(logfilename, 'a').close()
logging.basicConfig(    format='%(levelname)s - %(message)s',
                        filename=logfilename ,
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO
                        )
#Abriendo el archivo
path = os.path.abspath('data/directorio/directorio.csv')
df_csv = pd.read_csv(path)
df_csv = df_csv.sample(n=10000, random_state=1)

#ELIMINANDO LSO REGISTROS SIN PAGINA WEB
df_csv = df_csv.dropna(subset=['WEB'])


#ELIMINANDO COLUMNAS NO UTILIZABLES
del df_csv['Unnamed: 16']
del df_csv['CIIU_ID_CIIU_4']
del df_csv['CIIU_ID_CIIU']

#CONVIRTIENDO EN MINUSCULAS
df_csv = df_csv.apply(lambda x: x.str.lower() if x.dtype == "object" else x)

logging.info("Lista de Columnas despues da eliminar columnas no utilizadas")
logging.info(list(df_csv))

logging.info("Conteo de Columnas despues da eliminar columnas no utilizadas")
logging.info(df_csv.count())

#LISTANDO HOSTS REPETIDOS
duplicateRowsDF = df_csv[df_csv.duplicated(['WEB'])]
duplicateRowsDF = duplicateRowsDF.dropna(subset=['WEB'])
duplicateRowsDF['WEB'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con hostname repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#ELIMINANDO LOS HOSTS MAS REPETIDOS
arrayFindValues = ['0']

for index, row in df_csv.iterrows():
    if row['WEB'] in arrayFindValues:
        df_csv.at[index, 'WEB'] = np.nan


#LISTANDO NIT REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['NIT'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['NIT'])
duplicateRowsEmail['NIT'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con NIT repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#LISTANDO NOMBRE_COMERCIAL REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['NOMBRE_COMERCIAL'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['NOMBRE_COMERCIAL'])
duplicateRowsEmail['NOMBRE_COMERCIAL'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con NOMBRE_COMERCIAL repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#LISTANDO EMAIL REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['EMAIL'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['EMAIL'])
duplicateRowsEmail['EMAIL'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con EMAIL repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#LISTANDO TELEFONO1 REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['TELEFONO1'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['TELEFONO1'])
duplicateRowsEmail['TELEFONO1'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con TELEFONO1 repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#LISTANDO RAZON_SOCIAL REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['RAZON_SOCIAL'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['RAZON_SOCIAL'])
duplicateRowsEmail['RAZON_SOCIAL'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con RAZON_SOCIAL repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#CREANDO COLUMNA DE URL LIMPIA
df_csv['Web Page Main'] = df_csv['WEB']
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('www.','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('com.','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.co','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.gov','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.edu','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.org','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.net','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.io','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.ve','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.us','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.es','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.me','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.in','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.cl','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.pe','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.mx','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.we','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.uk','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.eu','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.xyz','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.ong','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.direct','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.book','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.info','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.site','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.blog','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.tv','')
df_csv['Web Page Main'] = df_csv['Web Page Main'].str.replace('.eu','')


i = 0
arrayHosts = []
for index, row in df_csv.iterrows():
    x = str(row['Web Page Main']).split(".")


# LIMPIANDO Y AGRUPANDO CAMPOS POR HOSTNAME
i = 0
json_relation = []
for index, row in df_csv.iterrows():
    hostname = row['Web Page Main']
    df_csv_copy =  df_csv
    df_filter = df_csv_copy['Web Page Main'].str.match(str(hostname))

    contains_string =  df_filter == True
    df_csv_filtered = df_csv_copy[contains_string]
    count = df_csv_filtered['Web Page Main'].count()
    if(count > 1):
        for index2, row2 in df_csv_filtered.iterrows():
            if row['Web Page Main'] == row2['Web Page Main'] and index != index2:
                relation = {
                    "indexClean" : index,
                    "indexRaw" : index2,
                    "hostGroup" : row['Web Page Main'],
                    "hostnameIndex" : row2['Web Page Main']
                }
                df_csv = df_csv.drop(index2)
                json_relation.append(relation)
                logging.info(relation)

cleanFilePath = 'data/directorio/directorio_limpio.csv'
if os.path.exists(cleanFilePath):
    os.remove(cleanFilePath)
df_csv.to_csv(r'data/directorio/directorio_limpio.csv')


#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla directorio_clean
postgres_delete_query = """ DELETE FROM directorio_clean"""
cursor.execute(postgres_delete_query)
connection.commit()

#GUARDANDO INFORMACION EN BD TABLA CLEAN
postgres_insert_query = """ INSERT INTO directorio_clean (id,tipo_documento, nit, digito_verificacion, razon_social, nombre_comercial, direccion, muni_id_dpto, nombre_dpto, muni_id_mpio ,nombre_mpio, telefono1, telefono2, web, email) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

i = 0
for index, row in df_csv.iterrows():
    record_to_insert = (i,row['TIPO_DOCUMENTO'],row['NIT'],row['DIGITO_VERIFICACION'],row['RAZON_SOCIAL'],row['NOMBRE_COMERCIAL'],row['DIRECCION'],row['MUNI_ID_DPTO'],row['NOMBRE_DPTO'],row['MUNI_ID_MPIO'],row['NOMBRE_MPIO'],row['TELEFONO1'],row['TELEFONO2'],row['WEB'],row['EMAIL'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + "Registros guardados en directorio_clean")


#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla directorio_clean
postgres_delete_query = """ DELETE FROM directorio_clean_raw"""
cursor.execute(postgres_delete_query)
connection.commit()

#GUARDANDO INFORMACION EN BD TABLA directorio_clean_raw
postgres_insert_query = """ INSERT INTO directorio_clean_raw (raw_id,clean_id) VALUES (%s,%s)"""

i = 0
for item in json_relation:
    record_to_insert = (item['indexRaw'], item['indexClean'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + " Registros guardados en directorio_clean_raw")
