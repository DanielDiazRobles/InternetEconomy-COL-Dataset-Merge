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
#df_csv = df_csv.sample(n=2000, random_state=1)

#ELIMINANDO COLUMNAS NO UTILIZABLES
del df_csv['Unnamed: 16']
del df_csv['CIIU_ID_CIIU_4']
del df_csv['CIIU_ID_CIIU']
del df_csv['TIPO_DOCUMENTO']
del df_csv['DIGITO_VERIFICACION']
del df_csv['DIRECCION']
del df_csv['MUNI_ID_DPTO']
del df_csv['NOMBRE_DPTO']
del df_csv['MUNI_ID_MPIO']
del df_csv['NOMBRE_MPIO']
del df_csv['TELEFONO1']
del df_csv['TELEFONO2']

df_csv['NIT'] = df_csv['NIT'].astype(str)
df_csv = df_csv.where((pd.notnull(df_csv)), "")


#ELIMINANDO LSO REGISTROS SIN PAGINA WEB
df_csv = df_csv.dropna(how='all')

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
logging.info(duplicateRowsDF['WEB'].value_counts())
logging.info("")

#ELIMINANDO LOS HOSTS MAS REPETIDOS
arrayFindValues = ['0']

for index, row in df_csv.iterrows():
    if row['WEB'] in arrayFindValues:
        df_csv.at[index, 'WEB'] = np.nan

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

#AGRUPANDO POR
group_nit = df_csv.groupby('NIT')


#CREANDO LA CADENA DE CONNECTION DE DIRECTORIO LIMPIO
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla directorio_clean
postgres_delete_query = """ DELETE FROM directorio_clean"""
cursor.execute(postgres_delete_query)
connection.commit()
postgres_insert_query = """ INSERT INTO directorio_clean (id, nit, razon_social, nombre_comercial, web, email) VALUES (%s,%s,%s,%s,%s,%s)"""

#1000 en 20 seg
#GUARDANDO EN LA TABLA LIMPIA Y GENERANDO LOS REGISTROS DE RELACIÓN
json_relation = []
new_index = 0;
for name_of_the_group, group in group_nit:
    count = 0;
    for index, row in group.iterrows():
        if count == 0:
            record_to_insert = (new_index, row['NIT'],row['RAZON_SOCIAL'],row['NOMBRE_COMERCIAL'],row['WEB'],row['EMAIL'])
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            count = 1
        else:
            relation = {
                "indexClean" : new_index,
                "indexRaw" : index,
            }
            json_relation.append(relation)
    new_index = new_index + 1
print(str(new_index) + "Registros guardados en directorio_clean")


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
    record_to_insert = (item['indexRaw'].item(), item['indexClean'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + " Registros guardados en directorio_clean_raw")


'''
cleanFilePath = 'data/directorio/directorio_limpio.csv'
if os.path.exists(cleanFilePath):
    os.remove(cleanFilePath)
df_csv.to_csv(r'data/directorio/directorio_limpio.csv')
'''
