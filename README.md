# (CEPAL-DANE) Internet Economy - Algoritmo Merge Datasets para Colombia

## Contexto

Ver el [1er Informe del proyecto](https://drive.google.com/file/d/15d87DzYCLdhIdmECGaGjo5fv0Q3eQ_No/view?usp=sharing) para conocer el contexto, metodologia y otra informaciòn introductoria del proyecto.

## Funcionamiento General

El algoritmo esta compuesto por diferentes scripts; estos scripts tienen un orden de ejecución definido y trabajan usando la información contenida en los csv's de la carpeta data o consultando datos de tablas especificas de la base de datos en la que se persiste la trazabilidad del proceso.

La base de datos se limpia cada vez que se ejecuta el proceso y contiene el resultado de la ultima ejecución del proceso. Su proposito es suministrar la información a los expertos tematicos para validar el algoritmo.

## Estructura de carpetas

### data

Carpeta en la que se encuentran la información de los dataset base.

### dbschema

Carpeta en la que se encuentra el script de creación de la base de datos que soporta las tareas a ejecutar.

### src

Carpeta en la que se encuentran los programas fuentes del aplicativo. Esta carpeta contiene un archivo run-tasks.py y una carpeta con las diferentes tareas ejecuta el aplcativo.

### logs

Carpeta en la que se guardan los logs de las ejecuciones de las tareas.

## Tareas a ejecutar

Para hacer merge de ambos dataset se ejecutan las siguientes tareas:

* 1.1-save-raw-dataprovider.py - Persiste el archivo "\data\dataprovider\dataprovider.csv" en la base de datos para dar trazabilidad al proceso de unificación de los registros.
* 1.2-clean-dataprovider.py - Limpia los registros del dataset y guarda en la base de datos los registros "limpios" y las relaciones de estos con el los registros del dataset original.
* 2.1-save-raw-dataprovider.py - Persiste el archivo "\data\directorio\directorio.csv" en la base de datos para dar trazabilidad al proceso de unificación de los registros.
* 2.2-clean-directory.py - Limpia los registros del dataset y guarda en la base de datos los registros "limpios" y las relaciones de estos con el los registros del dataset original.
* 3-merge-datasets.py - Unifica los registros limpios y los persiste en un archivo plano y en la base de datos para su trazabilidad. 

 ## Requisitos para ejecutar el proyecto

 El algoritmo esta desarrollado en Python - anaconda3-2019.07
 
 Las librerias a instalar se encuentran en el archivo requirements.txt para instalarlas ejecutar el siguiente comando.

```shell 
pip install -r requirements.txt
```




