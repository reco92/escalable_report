# Analisis de Transaciones 

Este repositorio contiene scripts y una arquitectura basada en kafka para realizar analisis de datos sobre un dataset de transacciones.  

## Local Setup

Es necesario tener Docker instalado, y crear un archivo `.env`, pueden basarse en los valores del archivo `ENV`

### Construir Imagenes y Levantarlas

```shell 
me@localhost# docker-compose up --build
```


### Kafka

EL servicio de Kafka se inicializa por defecto, se puede comprobar los topicos accediendo al contenedor y ejecutando el
comando:

```shell
kafka-topics.sh --list --bootstrap-server localhost:9092
```


### Ejecutar PysSpark

Acceder al conteneder de PySpark y Ejecutar este comando:

```shell
shell me@localhost#:/home/scripts# python json_stream.py
```


### Ejecutar Scripts

Acceder al conteneder de Producer para generar los mensajes iniciales:

```shell 
me@localhost#:/app# python producer_json.py
```


Acceder al conteneder de Consumer para procesar los mensajes registrados por Producer:

```shell 
me@localhost#:/app# python consumer_json.py
```

