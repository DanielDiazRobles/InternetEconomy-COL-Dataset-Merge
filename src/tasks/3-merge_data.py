import sys
import pandas as pd
import numpy as np
import os
import psycopg2
import logging
import Levenshtein as lev
from difflib import SequenceMatcher
from conf_env import environment
from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

logging.basicConfig(filename='logs/merge_data.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

#Consulta  a la BD tabla directorio_clean
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + environment.user_psql + "' host='localhost' password='" + environment.pass_psql +"'")
postgreSQL_select_Query = "SELECT * FROM directorio_clean"
cursor = connection.cursor()
cursor.execute(postgreSQL_select_Query)
directorio = cursor.fetchall()
df_directorio = pd.DataFrame(directorio, columns=['id','TIPO_DOCUMENTO','NIT','DIGITO_VERIFICACION','RAZON_SOCIAL','NOMBRE_COMERCIAL','DIRECCION','MUNI_ID_DPTO','NOMBRE_DPTO','MUNI_ID_MPIO','NOMBRE_MPIO','TELEFONO1','TELEFONO2','WEB','EMAIL','col2','col3'])
logging.warning(df_directorio.count())

#Consulta  a la BD tabla dataprovider_clean
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + environment.user_psql + "' host='localhost' password='" + environment.pass_psql +"'")
postgreSQL_select_Query = "SELECT * FROM dataprovider_clean"
cursor = connection.cursor()
cursor.execute(postgreSQL_select_Query)
dataprovider = cursor.fetchall()
df_dataprovider = pd.DataFrame(dataprovider, columns=['id', 'Hostname', 'Continent', 'Country', 'Region', 'Zip code', 'City', 'Address', 'Addresses', 'Company name', 'Company type', 'Company quality', 'Legal entity', 'Business Registry number', 'IBAN number', 'BIC number', 'Tax number', 'Phone number', 'Secondary phone numbers', 'Email address', 'Secondary email addresses', 'Keywords', 'Relevant keywords', 'Subdomain', 'Domain', 'DNS NS domain', 'main_web'])
logging.warning(df_dataprovider.count())

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
            Ratio = lev.ratio(row['Hostname'].lower(),row2['WEB'].lower())
            if Ratio > 0.80:
                merge = {
                    "web_page_dataprovider" : row['Web Page'],
                    "web_page_directorio" : row2['Web Page'],
                    "index_dataprovider" : row['id'],
                    "index_directorio" : row2['id'],
                    "ratio" : Ratio,
                    "item" : "hostname",
                    "value_comparation_dataprovider" : row['Web Page'],
                    "value_comparation_directorio" : row2['Web Page']
                }
                logging.warning(merge)
                result.append(merge)


result_name = []
df_directorio_lower = df_directorio.apply(lambda x: x.str.lower() if x.dtype == "object" else x)

for index, row in df_dataprovider.iterrows():
    name =  row['Company name'].lower()
    if name != "nan":
        df_csv_direcotrio_copy = df_directorio_lower
        try:
            df_filter = df_csv_direcotrio_copy['RAZON_SOCIAL'].str.match(name)
            contains_string =  df_filter == True
            df_csv__filtered = df_csv_direcotrio_copy[contains_string]
            count = df_csv__filtered['RAZON_SOCIAL'].count()
            if(count > 0):
                for index2, row2 in df_csv__filtered.iterrows():
                    Ratio = lev.ratio(row['Company name'].lower(),row2['RAZON_SOCIAL'].lower())
                if Ratio > 0.70:
                    merge = {
                        "web_page_dataprovider" : row['Web Page'],
                        "web_page_directorio" : row2['Web Page'],
                        "value_comparation_dataprovider" : row['Company name'],
                        "value_comparation_directorio" : row2['RAZON_SOCIAL'],
                        "index_dataprovider" : row['id'],
                        "index_directorio" : row2['id'],
                        "ratio" : Ratio,
                        "item" : "Razón Social"
                    }
                    logging.warning(merge)
                    result_name.append(merge)
                    #print(merge)
        except:
          print("An exception occurred")


        df_csv_direcotrio_copy = df_directorio_lower
        try:
            df_filter = df_csv_direcotrio_copy['NOMBRE_COMERCIAL'].str.match(name)
            contains_string =  df_filter == True
            df_csv__filtered = df_csv_direcotrio_copy[contains_string]
            count = df_csv__filtered['NOMBRE_COMERCIAL'].count()
            if(count > 0):
                for index2, row2 in df_csv__filtered.iterrows():
                    Ratio = lev.ratio(row['Company name'].lower(),row2['NOMBRE_COMERCIAL'].lower())
                if Ratio > 0.70:
                    merge = {
                        "web_page_dataprovider" : row['Web Page'],
                        "web_page_directorio" : row2['Web Page'],
                        "value_comparation_dataprovider" : row['Company name'],
                        "value_comparation_directorio" : row2['NOMBRE_COMERCIAL'],
                        "index_dataprovider" : row['id'],
                        "index_directorio" : row2['id'],
                        "ratio" : Ratio,
                        "item" : "Nombre Comercial"
                    }
                    logging.warning(merge)
                    result_name.append(merge)
        except:
          print("An exception occurred")


result_mail = []

for index, row in df_dataprovider.iterrows():
    main_email =  row['Email address']
    secondary_email =  row['Secondary email addresses']

    if main_email != "NaN":
        df_csv_direcotrio_copy = df_directorio
        df_filter = df_csv_direcotrio_copy['EMAIL'].str.match(main_email)
        contains_string =  df_filter == True
        df_csv__filtered = df_csv_direcotrio_copy[contains_string]
        count = df_csv__filtered['EMAIL'].count()
        if(count > 0):
            for index2, row2 in df_csv__filtered.iterrows():
                Ratio = lev.ratio(main_email.lower(),row2['EMAIL'].lower())
                if Ratio > 0.90:
                    merge = {
                        "web_page_dataprovider" : row['Web Page'],
                        "web_page_directorio" : row2['Web Page'],
                        "value_comparation_dataprovider" : row['Email address'],
                        "value_comparation_directorio" : row2['EMAIL'],
                        "index_dataprovider" : row['id'],
                        "index_directorio" : row2['id'],
                        "ratio" : Ratio,
                        "item" : "Email"
                    }
                    logging.warning(merge)
                    result_mail.append(merge)

    if secondary_email != "NaN":
        df_csv_direcotrio_copy = df_directorio
        df_filter = df_csv_direcotrio_copy['EMAIL'].str.match(secondary_email)
        contains_string =  df_filter == True
        df_csv__filtered = df_csv_direcotrio_copy[contains_string]
        count = df_csv__filtered['EMAIL'].count()
        if(count > 0):
            for index2, row2 in df_csv__filtered.iterrows():
                Ratio = lev.ratio(secondary_email.lower(),row2['EMAIL'].lower())
                if Ratio > 0.90:
                    merge = {
                        "web_page_dataprovider" : row['Web Page'],
                        "web_page_directorio" : row2['Web Page'],
                        "value_comparation_dataprovider" : row['Secondary email addresses'],
                        "value_comparation_directorio" : row2['EMAIL'],
                        "index_dataprovider" : row['id'],
                        "index_directorio" : row2['id'],
                        "ratio" : Ratio,
                        "item" : "Secondary Email"
                    }
                    logging.warning(merge)
                    result_mail.append(merge)
print(len(result_mail))


#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + environment.user_psql + "' host='localhost' password='" + environment.pass_psql +"'")
cursor = connection.cursor()

#Limpiando la tabla directorio_clean
postgres_delete_query = """ DELETE FROM merge_relations"""
cursor.execute(postgres_delete_query)
connection.commit()

#GUARDANDO INFORMACION EN BD TABLA CLEAN
postgres_insert_query = """ INSERT INTO merge_relations (merge_id,dataprovider_id,directorio_id,web_page_dataprovider,web_page_directorio,value_comparation_dataprovider,value_comparation_directorio,ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

i = 0
for item in result:
    record_to_insert = (i, item['index_dataprovider'], item['index_directorio'], item['web_page_dataprovider'], item['web_page_directorio'], item['value_comparation_dataprovider'], item['value_comparation_directorio'],item['ratio'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + " Registros guardados en merge_relations")
result.append(str(i) + " Registros guardados en merge_relations")
