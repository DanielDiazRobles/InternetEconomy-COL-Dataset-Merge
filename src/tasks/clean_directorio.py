import pandas as pd
import numpy as np
import os
#import psycopg2
from difflib import SequenceMatcher

#Abriendo el archivo
os.getcwd()
os.chdir("../..")
os.chdir("data/directorio")
path = os.path.abspath('directorio.csv')
df_csv = pd.read_csv(path)

#ELIMINANDO COLUMNAS NO UTILIZABLES
del df_csv['Unnamed: 16']
del df_csv['CIIU_ID_CIIU_4']
del df_csv['CIIU_ID_CIIU']

#CONVIRTIENDO EN MINUSCULAS
df_csv = df_csv.apply(lambda x: x.str.lower() if x.dtype == "object" else x)

print("Lista de Columnas en el nuevo dataframe");
print(list(df_csv))

print("Conteo de Columnas en el nuevo dataframe");
print(df_csv.count())

#LISTANDO HOSTS REPETIDOS
duplicateRowsDF = df_csv[df_csv.duplicated(['WEB'])]
print("Conteo de Columnas con hostname repetidos");
print("");
duplicateRowsDF = duplicateRowsDF.dropna(subset=['WEB'])
duplicateRowsDF['WEB'].value_counts()
print(duplicateRowsDF['WEB'].value_counts())

#ELIMINANDO LOS HOSTS MAS REPETIDOS
arrayFindValues = ['0']

for index, row in df_csv.iterrows():
    if row['WEB'] in arrayFindValues:
        df_csv.at[index, 'WEB'] = np.nan


#LISTANDO NIT REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['NIT'])]
print("Conteo de Columnas con NIT repetidos");
print("");
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['NIT'])
duplicateRowsEmail['NIT'].value_counts()
print(duplicateRowsEmail['NIT'].value_counts())


#LISTANDO NOMBRE_COMERCIAL REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['NOMBRE_COMERCIAL'])]
print("Conteo de Columnas con NIT repetidos");
print("");
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['NOMBRE_COMERCIAL'])
duplicateRowsEmail['NOMBRE_COMERCIAL'].value_counts()
print(duplicateRowsEmail['NOMBRE_COMERCIAL'].value_counts())


#LISTANDO EMAIL REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['EMAIL'])]
print("Conteo de Columnas con NIT repetidos");
print("");
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['EMAIL'])
duplicateRowsEmail['EMAIL'].value_counts()
print(duplicateRowsEmail['EMAIL'].value_counts())


#LISTANDO TELEFONO1 REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['TELEFONO1'])]
print("Conteo de Columnas con NIT repetidos");
print("");
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['TELEFONO1'])
duplicateRowsEmail['TELEFONO1'].value_counts()
print(duplicateRowsEmail['TELEFONO1'].value_counts())


#LISTANDO TELEFONO1 REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['RAZON_SOCIAL'])]
print("Conteo de Columnas con NIT repetidos");
print("");
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['RAZON_SOCIAL'])
duplicateRowsEmail['RAZON_SOCIAL'].value_counts()
print(duplicateRowsEmail['RAZON_SOCIAL'].value_counts())




# #Guardando la informaciòn en la BD
# for index, row in df_csv.iterrows():
#     record_to_insert = (i,row['TIPO_DOCUMENTO'],row['NIT'],row['DIGITO_VERIFICACION'],row['RAZON_SOCIAL'],row['NOMBRE_COMERCIAL'],row['DIRECCION'],row['MUNI_ID_DPTO'],row['NOMBRE_DPTO'],row['MUNI_ID_MPIO'],row['NOMBRE_MPIO'],row['TELEFONO1'],row['TELEFONO2'],row['WEB'],row['EMAIL'],row['CIIU_ID_CIIU'],row['CIIU_ID_CIIU_4'])
#     cursor.execute(postgres_insert_query, record_to_insert)
#     connection.commit()
#     count = cursor.rowcount
#     print(str(index) + " valores incluidos en la base de datos ")
