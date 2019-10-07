import pandas as pd
import numpy as np
import os
import psycopg2
import sys
import configparser

config = configparser.ConfigParser()
config.read("config.ini")


#Abriendo el archivo
path = os.path.abspath('data/directorio/directorio.csv')
df_csv = pd.read_csv(path)
#df_csv = df_csv.sample(n=1000, random_state=1)


#Generando la conexion a BD
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla directorio_raw
postgres_delete_query = """ DELETE FROM directorio_raw"""
cursor.execute(postgres_delete_query)
connection.commit()

postgres_insert_query = """ INSERT INTO directorio_raw (id,tipo_documento,nit,digito_verificacion,razon_social,nombre_comercial,direccion,muni_id_dpto,nombre_dpto,muni_id_mpio,nombre_mpio,telefono1,telefono2,web,email,ciiu_id_ciiu,ciiu_id_ciiu_4) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

i = 0
#Guardando la informaciòn en la BD
for index, row in df_csv.iterrows():
    record_to_insert = (i,row['TIPO_DOCUMENTO'],row['NIT'],row['DIGITO_VERIFICACION'],row['RAZON_SOCIAL'],row['NOMBRE_COMERCIAL'],row['DIRECCION'],row['MUNI_ID_DPTO'],row['NOMBRE_DPTO'],row['MUNI_ID_MPIO'],row['NOMBRE_MPIO'],row['TELEFONO1'],row['TELEFONO2'],row['WEB'],row['EMAIL'],row['CIIU_ID_CIIU'],row['CIIU_ID_CIIU_4'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print("Se insertaron exitosamente " + str(i) + " registros")
