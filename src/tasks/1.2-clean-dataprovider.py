import pandas as pd
import numpy as np
import os
import psycopg2
import logging
from difflib import SequenceMatcher
import psycopg2
import sys
import configparser
import time

config = configparser.ConfigParser()
config.read("config.ini")

logfilename = 'logs/clean_dataprovider_' + time.strftime("%Y%m%d%I%M%S") + '.log'
open(logfilename, 'a').close()
logging.basicConfig(    format='%(levelname)s - %(message)s',
                        filename=logfilename ,
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO
                        )

#Abriendo el archivo
path = os.path.abspath('data/dataprovider/dataprovider.csv')
df_csv = pd.read_csv(path)
df_csv = df_csv.sample(n=1000, random_state=1)
df_csv = df_csv.where((pd.notnull(df_csv)), "")


#ELIMINANDO COLUMNAS NO UTILIZABLES
del df_csv['Zip code quality']
del df_csv['Phone number certainty']
del df_csv['Contactable']
del df_csv['Response']
del df_csv['Title']
del df_csv['Description']
del df_csv['Word count']
del df_csv['Website type']
del df_csv['Category']
del df_csv['SIC code']
del df_csv['SIC major group']
del df_csv['SIC division']
del df_csv['Copyright']
del df_csv['Logo']
del df_csv['Language']
del df_csv['Multi language']
del df_csv['Authors']
del df_csv['Privacy sensitive']
del df_csv['Pages']
del df_csv['Pages indexed']
del df_csv['Page types']
del df_csv['HTML size (kb)']
del df_csv['Date first found']
del df_csv['Date last analyzed']
del df_csv['Online store']
del df_csv['eCommerce certainty']
del df_csv['Shopping cart system']
del df_csv['Trustmarks']
del df_csv['Delivery services']
del df_csv['Payment methods']
del df_csv['Payment providers']
del df_csv['Currency']
del df_csv['Economic footprint']
del df_csv['Economic footprint delta']
del df_csv['Heartbeat']
del df_csv['Changes']
del df_csv['Incoming links']
del df_csv['Outgoing links']
del df_csv['Site traffic estimation']
del df_csv['Site traffic estimation (Average)']
del df_csv['Analytics ID']
del df_csv['AdSense ID']
del df_csv['Brand names']
del df_csv['Analytics']
del df_csv['Ad network']
del df_csv['Affiliate']
del df_csv['Social media types']
del df_csv['Social media widgets']
del df_csv['Social media profiles']
del df_csv['Live chat software']
del df_csv['CRM']
del df_csv['Email services']
del df_csv['Chain hash']
del df_csv['Chain count']
del df_csv['TLD suggestions']
del df_csv['CMS']
del df_csv['Scripting language']
del df_csv['Technical evaluation']
del df_csv['SEO score']
del df_csv['SEO summary']
del df_csv['Flash']
del df_csv['HTML version']
del df_csv['Generator']
del df_csv['Mobile version']
del df_csv['Mobile app']
del df_csv['Maps']
del df_csv['Libraries']
del df_csv['Scanrequest ID']
del df_csv['Top-level domain']
del df_csv['Top-level domain type']
del df_csv['New Generic Top-level domain category']
del df_csv['Domain name length']
del df_csv['IDN']
del df_csv['IDN parts']
del df_csv['Linked subdomains']
del df_csv['Linked subdomains count']
del df_csv['Resolved IP count']
del df_csv['Redirect hostname']
del df_csv['Redirect top-level domain']
del df_csv['Forwarding domains']
del df_csv['Forwarding domains count']
del df_csv['Hosting country']
del df_csv['IP address']
del df_csv['IPv6 address']
del df_csv['AS number']
del df_csv['Hosting company']
del df_csv['Registrar']
del df_csv['Reseller']
del df_csv['DNS NS Nameservers']
del df_csv['DNS MX domain']
del df_csv['DNS TXT']
del df_csv['DNSSEC']
del df_csv['DNS CNAME']
del df_csv['Operating system']
del df_csv['Webserver']
del df_csv['HTTP Headers']
del df_csv['Control panel']
del df_csv['Server signature']
del df_csv['SSL certificate']
del df_csv['SSL issuer organization']
del df_csv['SSL issuer common name']
del df_csv['SSL start date']
del df_csv['SSL end date']
del df_csv['SSL RSA key length']
del df_csv['SSL algorithm']
del df_csv['SSL type']
del df_csv['Status codes']
del df_csv['Average load time (ms)']
del df_csv['CDN']
del df_csv['Video']
del df_csv['Parking']
del df_csv['Placeholder']

logging.info("Lista de Columnas despues da eliminar columnas no utilizadas")
logging.info(list(df_csv))

logging.info("Conteo de Columnas despues da eliminar columnas no utilizadas")
logging.info(df_csv.count())
#LISTANDO HOSTS REPETIDOS
duplicateRowsDF = df_csv[df_csv.duplicated(['Hostname'])]
logging.info("")
logging.info("Conteo de Columnas con hostname repetidos")
logging.info(duplicateRowsDF.count())
logging.info("")


#LISTANDO CORREOS REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['Email address'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['Email address'])
duplicateRowsEmail['Email address'].value_counts()
logging.info("")
logging.info("Conteo de Columnas con EMAIL repetidos")
logging.info(duplicateRowsEmail['Email address'].value_counts())
logging.info("")


#ELIMINANDO LOS CORREOS MAS REPETIDOS
i = 0
arrayFindValues = ['info@example.com' , 'mail@correo.com' , 'email@yourbusiness.com' , 'info@demolink.org' , 'mail@example.com' , 'info@yourdomain.com' , 'info@yoursite.com' , 'contact@example.com' , 'info@misitio.com' , 'contact@company.com' , 'email@example.com' , 'info@domain.com' , 'info@company.com' , 'support@rn.org' , 'mail@demolink.org' , 'sales@yourcompany.com' , 'info@site.info' , 'info@yourwebsite.com' , 'email@domain.com' , 'info@corferias.com' , 'info@email.com' , 'contact@yoursite.com' , 'info@your-domain.com' , 'email@ejemplo.com' , 'info@felin , x.com.co' , 'support@example.com' , 'contact@email.com', 'mail@mail.com', 'mail@ejemplo.com', 'info@gmail.com', 'info@yourcompany.com', 'info@mail.com', 'info@mysite.com', 'info@sitename.com', 'marketing@example.com', 'support@company.com', 'info@website.com', 'wordpress@example.com', 'su@email.com', 'ejemplo@correo.com', 'your@email.com', 'nombre@correo.com', 'su-email@ejemplo.com', 'someone@example.com', 'example@email.com', 'youremail@yourdomain.com', 'correo@dominio.com', 'info@tudominio.com', 'ejemplo@ejemplo.com', 'name@example.com', 'ejemplo@gmail.com', 'example@example.com']
for index, row in df_csv.iterrows():
    if row['Email address'] in arrayFindValues:
        df_csv.at[index, 'Email address'] = np.nan
        i = i + 1
print(str(i) + " Correos corregidos")
logging.info(str(i) + " Correos corregidos")

#LISTA DE CORREOS DESPUES DE ELIMINAR LOS MÀS REPETIDOS
duplicateRowsEmail = df_csv[df_csv.duplicated(['Email address'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['Email address'])
duplicateRowsEmail['Email address'].value_counts()
logging.info("")
logging.info("Nueva lista de correos màs repetidos")
logging.info(duplicateRowsEmail['Email address'].value_counts())
logging.info("")


#DATAFRAME DESPUES DE ELIMINAR CORREOS REPETIDOS EN EMAIL
duplicateRowsEmail = df_csv[df_csv.duplicated(['Secondary email addresses'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['Secondary email addresses'])
duplicateRowsEmail['Secondary email addresses'].value_counts()
logging.info("")
logging.info("Nueva lista de correos màs repetidos del field SECONDARY EMAIL")
logging.info(duplicateRowsEmail['Secondary email addresses'].value_counts())
logging.info("")


#ELIMINANDO LOS CORREOS MAS REPETIDOS SECONDARY EMAIL
i = 0
arrayFindValues = ['info@example.com' , 'mail@correo.com' , 'email@yourbusiness.com' , 'info@demolink.org' , 'mail@example.com' , 'info@yourdomain.com' , 'info@yoursite.com' , 'contact@example.com' , 'info@misitio.com' , 'contact@company.com' , 'email@example.com' , 'info@domain.com' , 'info@company.com' , 'support@rn.org' , 'mail@demolink.org' , 'sales@yourcompany.com' , 'info@site.info' , 'info@yourwebsite.com' , 'email@domain.com' , 'info@corferias.com' , 'info@email.com' , 'contact@yoursite.com' , 'info@your-domain.com' , 'email@ejemplo.com' , 'info@felin , x.com.co' , 'support@example.com' , 'contact@email.com', 'mail@mail.com', 'mail@ejemplo.com', 'info@gmail.com', 'info@yourcompany.com', 'info@mail.com', 'info@mysite.com', 'info@sitename.com', 'marketing@example.com', 'support@company.com', 'info@website.com', 'wordpress@example.com', 'su@email.com', 'ejemplo@correo.com', 'your@email.com', 'nombre@correo.com', 'su-email@ejemplo.com', 'someone@example.com', 'example@email.com', 'youremail@yourdomain.com', 'correo@dominio.com', 'info@tudominio.com', 'ejemplo@ejemplo.com', 'name@example.com', 'ejemplo@gmail.com', 'example@example.com']
for index, row in df_csv.iterrows():
    if row['Secondary email addresses'] in arrayFindValues:
        df_csv.at[index, 'Secondary email addresses'] = np.nan
        i = i + 1
print(str(i) + " Correos corregidos")
logging.info(str(i) + " Correos corregidos")


#DATAFRAME DESPUES DE ELIMINAR CORREOS REPETIDOS EN SECONDARY EMAIL
duplicateRowsEmail = df_csv[df_csv.duplicated(['Secondary email addresses'])]
duplicateRowsEmail = duplicateRowsEmail.dropna(subset=['Secondary email addresses'])
duplicateRowsEmail['Secondary email addresses'].value_counts()
logging.info("")
logging.info("Nueva lista de correos màs repetidos del field SECONDARY EMAIL")
logging.info(duplicateRowsEmail['Secondary email addresses'].value_counts())
logging.info("")


#CREANDO COLUMNA DE URL LIMPIA
df_csv['Web Page'] = df_csv['Hostname']
df_csv['Hostname'] = df_csv['Hostname'].str.replace('www.','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.com','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.co','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.gov','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.edu','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.org','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.net','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.io','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.ve','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.us','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.es','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.me','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.in','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.cl','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.pe','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.mx','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.we','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.uk','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.eu','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.xyz','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.ong','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.direct','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.book','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.info','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.site','')
df_csv['Hostname'] = df_csv['Hostname'].str.replace('.blog','')

print(df_csv.count())
#AGRUPANDO por hostname
group_host = df_csv.groupby('Hostname')
print(group_host.count())
#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla dataprovider_clean
postgres_delete_query = """ DELETE FROM dataprovider_clean"""
cursor.execute(postgres_delete_query)
connection.commit()
postgres_insert_query = """ INSERT INTO dataprovider_clean (id,hostname, continent, country, region, zip_code, city, address, addresses, company_name ,company_type, company_quality, legal_entity, business_registry_number, iban_number, bic_number, tax_number, phone_number, secondary_phone_numbers, email_address, secondary_email_addresses, keywords, relevant_keywords, subdomain, domain, dns_ns_domain) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


#GUARDANDO EN LA TABLA LIMPIA Y GENERANDO LOS REGISTROS DE RELACIÓN
json_relation = []
new_index = 0;
for name_of_the_group, group in group_host:
    count = 0;
    for index, row in group.iterrows():
        if count == 0:
            record_to_insert = (index.item(), row['Hostname'], row['Continent'], row['Country'], row['Region'], row['Zip code'], row['City'], row['Address'], row['Addresses'], row['Company name'], row['Company type'], row['Company quality'], row['Legal entity'], row['Business Registry number'], row['IBAN number'], row['BIC number'], row['Tax number'], row['Phone number'], row['Secondary phone numbers'], row['Email address'], row['Secondary email addresses'], row['Keywords'], row['Relevant keywords'], row['Subdomain'], row['Domain'], row['DNS NS domain'])
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            count = 1
        else:
            relation = {
                "indexClean" : new_index,
                "indexRaw" : index.item(),
            }
            json_relation.append(relation)
    new_index = new_index + 1
print(str(new_index) + " Registros guardados en dataprovider_clean")




#CREANDO LA CADENA DE CONNECTION
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla dataprovider_clean_raw
postgres_delete_query = """ DELETE FROM dataprovider_clean_raw"""
cursor.execute(postgres_delete_query)
connection.commit()

postgres_insert_query = """ INSERT INTO dataprovider_clean_raw (raw_id,clean_id) VALUES (%s,%s)"""

i = 0
for item in json_relation:
    record_to_insert = (item['indexRaw'], item['indexClean'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print(str(i) + " Registros guardados en dataprovider_clean_raw")



'''
cleanFilePath = 'data/dataprovider/dataprovider_limpio.csv'
if os.path.exists(cleanFilePath):
    os.remove(cleanFilePath)
df_csv.to_csv(r'data/dataprovider/dataprovider_limpio.csv')
'''
