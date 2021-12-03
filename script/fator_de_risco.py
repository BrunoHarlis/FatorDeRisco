# -*- coding: utf-8 -*-
# Scripty para realização de análize de fator de risco de motorista em uma empresa de transporte.
#
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# CRIANDO SPARK SESSION
#----------------------------------------------------------------------
print("INICIANDO OS TRABALHOS...")
hiveContext = SparkSession.builder.appName("FatordeRisco").getOrCreate()
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)



# SCHEMA PARA MOTORISTASMILHAS
#----------------------------------------------------------------------
motoristaMilhasSchema = StructType().add("motoristaid", "string", True).add("totmilhas", "double", True)



# CRIANDO RDDs A PARTIR DE DADOS DO HDFS
#----------------------------------------------------------------------
geolocalizacaoDF = spark.read.csv('hdfs:///tmp/data/geolocalizacao/geolocalizacao.csv', header=True)
motoristaMilhasDF = spark.read.csv('hdfs:///tmp/data/motoristamilhas/motoristamilhas.csv', header=True, schema=motoristaMilhasSchema)



# CRIANDO TABELAS TEMPORÁRIAS
#----------------------------------------------------------------------
geolocalizacaoDF.createOrReplaceTempView("geolocalizacao")

print("\tTABELA GEOLOCALIZAÇÃO")
hiveContext.sql("SELECT * FROM geolocalizacao LIMIT 15").show()

#----------------------------------------------------------------------
motoristaMilhasDF.createOrReplaceTempView("motoristamilhas")

print("\tTABELA MOTORISTAMILHAS")
hiveContext.sql("SELECT * FROM motoristamilhas LIMIT 15").show()

#----------------------------------------------------------------------
geolocalizacao_temp0 = hiveContext.sql("SELECT * FROM geolocalizacao")
motoristamilhas_temp0 = hiveContext.sql("SELECT * FROM motoristamilhas")

geolocalizacao_temp0.createOrReplaceTempView("geolocalizacao_temp0")
motoristamilhas_temp0.createOrReplaceTempView("motoristamilhas_temp0")



# FILTRAGEM DE EVENTOS ANORMAIS
#----------------------------------------------------------------------
print("FILTRANDO EVENTOS ANORMAIS")
geolocalizacao_temp1 = hiveContext.sql("SELECT driverid, COUNT(driverid) occurance FROM geolocalizacao_temp0 WHERE event!='normal' GROUP BY driverid")
geolocalizacao_temp1.createOrReplaceTempView("geolocalizacao_temp1")

print("TABELA DE MOTORISTAS COM EVENTOS ANORMAIS")
hiveContext.sql("SELECT * FROM geolocalizacao_temp1 LIMIT 15").show()



# REALIZANDO JUNÇÃO ENTRE AS TABELAS GEOLOCALIZACAO_TEM1 E MOTORISTAMILHAS_TEMP0
#----------------------------------------------------------------------
juncao = hiveContext.sql("SELECT a.driverid, a.occurance, b.totmilhas FROM geolocalizacao_temp1 a, motoristamilhas_temp0 b WHERE a.driverid=b.motoristaid")
juncao.createOrReplaceTempView("juncao")



# INICIANDO ANÁLIZE DE FATOR DE RISCO
#----------------------------------------------------------------------
fator_de_risco = hiveContext.sql("SELECT driverid,occurance,totmilhas, totmilhas/occurance fatorrisco FROM juncao")
fator_de_risco.createOrReplaceTempView("fator_de_risco")

print("\tTABELA FATOR DE RISCO")
hiveContext.sql("SELECT * FROM fator_de_risco LIMIT 15").show()



# TODAS AS TABELAS TEMPORÁRIAS CRIADAS
#----------------------------------------------------------------------
print("\tTABELAS DO BANCO DE DADOS")
hiveContext.sql("SHOW TABLES").show()



# SALVANDO O RESULTADO EM UM ARQUIVO CSV
#----------------------------------------------------------------------
fator_de_risco.coalesce(1).write.csv("hdfs:///tmp/data/fatorderisco")


print("FIM DOS TRABALHOS\n\n")

spark.stop()
