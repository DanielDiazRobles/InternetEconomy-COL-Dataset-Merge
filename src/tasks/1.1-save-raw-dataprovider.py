import pandas as pd
import numpy as np
import os
import psycopg2
import sys
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

#Abriendo el archivo
path = os.path.abspath('data/dataprovider/dataprovider.csv')
df_csv = pd.read_csv(path)
#df_csv = df_csv.sample(n=1000, random_state=1)


#Generando la conexion a BD
connection = psycopg2.connect("dbname='cd_digital_economy' user='" + config['DataBase']['user'] + "' host='localhost' password='" + config['DataBase']['password'] +"'")
cursor = connection.cursor()

#Limpiando la tabla dataprovider_raw
postgres_delete_query = """ DELETE FROM dataprovider_raw"""
cursor.execute(postgres_delete_query)
connection.commit()


postgres_insert_query = """ INSERT INTO dataprovider_raw (id,hostname,continent,country,region,zip_code_quality,zip_code,city,address,addresses,company_name,company_type,company_quality,legal_entity_number,business_registry,iban_number,bic_number,tax_number,phone_number_certainty,phone_number_numbers,secondary_phone,email_address_addresses,secondary_email,contactable,response,title,description,keywords,relevant_keywords,word_count,website_type,category,sic_code_group,sic_major,sic_division,copyright,logo,language,multi_language,authors,privacy_sensitive,pages,pages_indexed,page_types_kb,html_size_found,date_first_analyzed,date_last,online_store,ecommerce_certainty_system,shopping_cart,trustmarks,delivery_services,payment_methods,payment_providers,currency,economic_footprint_delta,economic_footprint,heartbeat,changes,incoming_links,outgoing_links_estimation,site_traffic_estimation,site_traffic__average,analytics_id,adsense_id,brand_names,analytics,ad_network,affiliate__types,social_media_widgets,social_media_profiles,social_media_software,Live_chat,email_services,chain_hash,chain_count,tld_suggestions,cms,scripting_language,technical_evaluation,seo_score,seo_summary,flash,html_version,generator,mobile_version,mobile_app,maps,libraries,scanrequest_id,top_level_domain_type,top_level_domain_top_level,new_generic__domain_category,subdomain,domain__length,domain_name,idn,idn_parts,linked_subdomains_count,linked_subdomains_count__1,resolved_ip,redirect_hostname_domain,redirect_top_level,forwarding_domains_count,forwarding_domains,hosting_country,ip_address,ipv6_address,as_number,hosting_company,registrar,reseller__domain,dns_ns_nameservers,dns_ns_domain,dns_mx,dns_txt,dnssec,dns_cname,operating_system,webserver,http_headers,control_panel,server_signature,ssl_certificate_organization,ssl_issuer_common,ssl_issuer_date_name,ssl_start_date,ssl_end_key,ssl_rsa__length,ssl_algorithm,ssl_type,status_codes_time,average_load__ms,cdn,video,parking,placeholder) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

i = 0
#Guardando la informaciòn en la BD
for index, row in df_csv.iterrows():
    record_to_insert = (i, row['Hostname'], row['Continent'], row['Country'], row['Region'], row['Zip code'], row['Zip code quality'], row['City'], row['Address'], row['Addresses'], row['Company name'], row['Company type'], row['Company quality'], row['Legal entity'], row['Business Registry number'], row['IBAN number'], row['BIC number'], row['Tax number'], row['Phone number'], row['Phone number certainty'], row['Secondary phone numbers'], row['Email address'], row['Secondary email addresses'], row['Contactable'], row['Response'], row['Title'], row['Description'], row['Keywords'], row['Relevant keywords'], row['Word count'], row['Website type'], row['Category'], row['SIC code'], row['SIC major group'], row['SIC division'], row['Copyright'], row['Logo'], row['Language'], row['Multi language'], row['Authors'], row['Privacy sensitive'], row['Pages'], row['Pages indexed'], row['Page types'], row['HTML size (kb)'], row['Date first found'], row['Date last analyzed'], row['Online store'], row['eCommerce certainty'], row['Shopping cart system'], row['Trustmarks'], row['Delivery services'], row['Payment methods'], row['Payment providers'], row['Currency'], row['Economic footprint'], row['Economic footprint delta'], row['Heartbeat'], row['Changes'], row['Incoming links'], row['Outgoing links'],row['Site traffic estimation'], row['Site traffic estimation (Average)'], row['Analytics ID'], row['AdSense ID'], row['Brand names'],row['Analytics'], row['Ad network'], row['Affiliate'], row['Social media types'], row['Social media widgets'], row['Social media profiles'], row['Live chat software'], row['Email services'], row['Chain hash'], row['Chain count'], row['TLD suggestions'], row['CMS'], row['Scripting language'], row['Technical evaluation'], row['SEO score'], row['SEO summary'], row['Flash'], row['HTML version'], row['Generator'], row['Mobile version'], row['Mobile app'], row['Maps'], row['Libraries'], row['Scanrequest ID'], row['Top-level domain'], row['Top-level domain type'], row['New Generic Top-level domain category'], row['Subdomain'], row['Domain'], row['Domain name length'], row['IDN'], row['IDN parts'], row['Linked subdomains'], row['Linked subdomains count'], row['Resolved IP count'], row['Redirect hostname'], row['Redirect top-level domain'], row['Forwarding domains'], row['Forwarding domains count'], row['Hosting country'],row['IP address'], row['IPv6 address'], row['AS number'], row['Hosting company'], row['Registrar'], row['Reseller'], row['DNS NS domain'], row['DNS NS Nameservers'], row['DNS MX domain'], row['DNS TXT'], row['DNSSEC'], row['DNS CNAME'], row['Operating system'], row['Webserver'], row['HTTP Headers'], row['Control panel'], row['Server signature'], row['SSL certificate'], row['SSL issuer organization'], row['SSL issuer common name'], row['SSL start date'], row['SSL end date'], row['SSL RSA key length'], row['SSL algorithm'], row['SSL type'], row['Status codes'], row['Average load time (ms)'], row['CDN'], row['Video'], row['Parking'], row['Placeholder'])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    i = i + 1
print("Se insertaron exitosamente " + str(i) + " registros")
