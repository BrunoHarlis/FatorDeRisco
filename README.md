# FatorDeRisco
Análise de caso de uso usando Hadoop, Hive e Spark.


Nesse projeto será realizada uma análise de caso de uso em uma frota de caminhões. Cada caminhão possui um equipamento de registro de eventos que é enviado para um datacenter onde será realizado processos dos dados para esclarecer o fator de risco de motoristas da frota.

## Carregando os dados no HDFS

O primeiro passo será carregar os dados colhidos pelos sensorres dos caminhões no HDFS. São os arquivos:

__geolocalizacao.csv__ - Possui os dados de geolocalização dos caminhões. Contem as colunas localização caminhão, data, hora, evento, veloidade, etc.

__caminhoes.csv__ - Possui informações sobre o modelo de caminhão, motoristaid, caminhaoid e informações de milhagem.

#### Criar uma pasta no hdfs para armazenar os dados
```
hdfs dfs -mkdir -p /tmp/data/{geolocalizacao,caminhoes}
```

#### Definir permissão de gravação no diretório data
```
hdfs dfs -chmod 777 /tmp/data
```

#### Carregando os dados
```
hdfs dfs -put geolocalizacao.csv /tmp/data/geolocalizacao
hdfs dfs -put caminhoes.csv /tmp/data/caminhoes
```

## Criar tabelas no Hive

Agora que temos os dados no HDFS, vamos criar tabelas e carrega-las com os dados de geolocalização e caminhões. As tabelas serão criadas no banco de dados default.

#### Criando a tabela externa "ext_geolocalizacao"
```
CREATE EXTERNAL TABLE ext_geolocalizacao(
caminhaoid STRING,
motoristaid STRING, 
evento STRING, 
latitude DOUBLE, 
longitude DOUBLE, 
cidade STRING,
estado STRING,
velocidade INT,
event_ind INT,
idling_ind INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '/tmp/data/geolocalizacao'
TBLPROPERTIES("skip.header.line.count"="1");
```

#### Criando a tabela gerenciada "geolocalizacao"
```
CREATE TABLE geolocalizacao(
caminhaoid STRING,
motoristaid STRING, 
evento STRING, 
latitude DOUBLE, 
longitude DOUBLE, 
cidade STRING,
estado STRING,
velocidade INT,
event_ind INT,
idling_ind INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"
STORED AS ORC;
```

#### Inserindo dados da tabela "ext_geolocalizacao" à tabela "geolocalizacao"
```
INSERT INTO geolocalizacao 
SELECT * FROM ext_geolocalizacao;
```

#### Criando a tabela externa "ext_caminhoes"
```
CREATE EXTERNAL TABLE ext_caminhoes(
motoristaid STRING,caminhaoid STRING,modelo STRING,jun13_miles INT,jun13_gas INT,may13_miles INT,may13_gas INT,apr13_miles INT,apr13_gas INT,mar13_miles INT,mar13_gas INT,feb13_miles INT,feb13_gas INT,jan13_miles INT,jan13_gas INT,dec12_miles INT,dec12_gas INT,nov12_miles INT,nov12_gas INT,oct12_miles INT,oct12_gas INT,sep12_miles INT,sep12_gas INT,aug12_miles INT,aug12_gas INT,jul12_miles INT,jul12_gas INT,jun12_miles INT,jun12_gas INT,may12_miles INT,may12_gas INT,apr12_miles INT,apr12_gas INT,mar12_miles INT,mar12_gas INT,feb12_miles INT,feb12_gas INT,jan12_miles INT,jan12_gas INT,dec11_miles INT,dec11_gas INT,nov11_miles INT,nov11_gas INT,oct11_miles INT,oct11_gas INT,sep11_miles INT,sep11_gas INT,aug11_miles INT,aug11_gas INT,jul11_miles INT,jul11_gas INT,jun11_miles INT,jun11_gas INT,may11_miles INT,may11_gas INT,apr11_miles INT,apr11_gas INT,mar11_miles INT,mar11_gas INT,feb11_miles INT,feb11_gas INT,jan11_miles INT,jan11_gas INT,dec10_miles INT,dec10_gas INT,nov10_miles INT,nov10_gas INT,oct10_miles INT,oct10_gas INT,sep10_miles INT,sep10_gas INT,aug10_miles INT,aug10_gas INT,jul10_miles INT,jul10_gas INT,jun10_miles INT,jun10_gas INT,may10_miles INT,may10_gas INT,apr10_miles INT,apr10_gas INT,mar10_miles INT,mar10_gas INT,feb10_miles INT,feb10_gas INT,jan10_miles INT,jan10_gas INT,dec09_miles INT,dec09_gas INT,nov09_miles INT,nov09_gas INT,oct09_miles INT,oct09_gas INT,sep09_miles INT,sep09_gas INT,aug09_miles INT,aug09_gas INT,jul09_miles INT,jul09_gas INT,jun09_miles INT,jun09_gas INT,may09_miles INT,may09_gas INT,apr09_miles INT,apr09_gas INT,mar09_miles INT,mar09_gas INT,feb09_miles INT,feb09_gas INT,jan09_miles INT,jan09_gas INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/tmp/data/caminhoes'
TBLPROPERTIES ("skip.header.line.count"="1");
```


#### Criando a tabela gerenciada "caminhoes"
```
CREATE TABLE caminhoes(
motoristaid STRING,caminhaoid STRING,modelo STRING,jun13_miles INT,jun13_gas INT,may13_miles INT,may13_gas INT,apr13_miles INT,apr13_gas INT,mar13_miles INT,mar13_gas INT,feb13_miles INT,feb13_gas INT,jan13_miles INT,jan13_gas INT,dec12_miles INT,dec12_gas INT,nov12_miles INT,nov12_gas INT,oct12_miles INT,oct12_gas INT,sep12_miles INT,sep12_gas INT,aug12_miles INT,aug12_gas INT,jul12_miles INT,jul12_gas INT,jun12_miles INT,jun12_gas INT,may12_miles INT,may12_gas INT,apr12_miles INT,apr12_gas INT,mar12_miles INT,mar12_gas INT,feb12_miles INT,feb12_gas INT,jan12_miles INT,jan12_gas INT,dec11_miles INT,dec11_gas INT,nov11_miles INT,nov11_gas INT,oct11_miles INT,oct11_gas INT,sep11_miles INT,sep11_gas INT,aug11_miles INT,aug11_gas INT,jul11_miles INT,jul11_gas INT,jun11_miles INT,jun11_gas INT,may11_miles INT,may11_gas INT,apr11_miles INT,apr11_gas INT,mar11_miles INT,mar11_gas INT,feb11_miles INT,feb11_gas INT,jan11_miles INT,jan11_gas INT,dec10_miles INT,dec10_gas INT,nov10_miles INT,nov10_gas INT,oct10_miles INT,oct10_gas INT,sep10_miles INT,sep10_gas INT,aug10_miles INT,aug10_gas INT,jul10_miles INT,jul10_gas INT,jun10_miles INT,jun10_gas INT,may10_miles INT,may10_gas INT,apr10_miles INT,apr10_gas INT,mar10_miles INT,mar10_gas INT,feb10_miles INT,feb10_gas INT,jan10_miles INT,jan10_gas INT,dec09_miles INT,dec09_gas INT,nov09_miles INT,nov09_gas INT,oct09_miles INT,oct09_gas INT,sep09_miles INT,sep09_gas INT,aug09_miles INT,aug09_gas INT,jul09_miles INT,jul09_gas INT,jun09_miles INT,jun09_gas INT,may09_miles INT,may09_gas INT,apr09_miles INT,apr09_gas INT,mar09_miles INT,mar09_gas INT,feb09_miles INT,feb09_gas INT,jan09_miles INT,jan09_gas INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"
STORED AS ORC;
```

#### Adicionando os dados da tabela "ext_caminhoes" à tabela "caminhoes"
```
INSERT INTO caminhoes
SELECT * FROM ext_caminhoes;
```

#### Exluindo as tabelas externas "ext_geolocalizacao" e "ext_caminhoes"
```
DROP TABLE ext_geolocalizacao;
DROP TABLE ext_caminhoes;
```

Até agora somente preparamos os dados para podermos efetivamente começar a realizar algumas análizes. Nosso objetivo principal será esclarecer os riscos que a empresa corre devido o cansaço dos motoristas, caminhões usados e o impacto de vários eventos de transporte sobre o risco.

Vamos começar calculando a quantidade de milhas por galão que cada caminhão consome. Começaremos com nossa tabela de dados de caminhões. Precisamos somar todas as milhas e colunas de combustível por caminhão. O Hive tem uma série de funções que podem ser usadas para reformatar uma tabela. A palavra-chave LATERAL VIEW é como invocamos as coisas. A função stack() nos permite reestruturar os dados em 3 colunas rotuladas data, milhas, combustivel (ex: 'june13', june13_miles, june13_gas) que perfazem um máximo de 54 linhas. Escolhemos caminhaoid, motoristaid, data, milhas, combustivel de nossa tabela original e adicionamos uma coluna chamada mpg que calculada a milhagem média (milhas/combustivel).

```
CREATE TABLE milhascaminhao 
STORED AS ORC 
AS 
SELECT caminhaoid, motoristaid, data, milhas, combustivel, milhas / combustivel mpg 
FROM caminhoes LATERAL VIEW stack(
54,'jun13',jun13_miles,jun13_gas,'may13',may13_miles,may13_gas,'apr13',apr13_miles,apr13_gas,'mar13',mar13_miles,mar13_gas,'feb13',feb13_miles,feb13_gas,'jan13',jan13_miles,jan13_gas,'dec12',dec12_miles,dec12_gas,'nov12',nov12_miles,nov12_gas,'oct12',oct12_miles,oct12_gas,'sep12',sep12_miles,sep12_gas,'aug12',aug12_miles,aug12_gas,'jul12',jul12_miles,jul12_gas,'jun12',jun12_miles,jun12_gas,'may12',may12_miles,may12_gas,'apr12',apr12_miles,apr12_gas,'mar12',mar12_miles,mar12_gas,'feb12',feb12_miles,feb12_gas,'jan12',jan12_miles,jan12_gas,'dec11',dec11_miles,dec11_gas,'nov11',nov11_miles,nov11_gas,'oct11',oct11_miles,oct11_gas,'sep11',sep11_miles,sep11_gas,'aug11',aug11_miles,aug11_gas,'jul11',jul11_miles,jul11_gas,'jun11',jun11_miles,jun11_gas,'may11',may11_miles,may11_gas,'apr11',apr11_miles,apr11_gas,'mar11',mar11_miles,mar11_gas,'feb11',feb11_miles,feb11_gas,'jan11',jan11_miles,jan11_gas,'dec10',dec10_miles,dec10_gas,'nov10',nov10_miles,nov10_gas,'oct10',oct10_miles,oct10_gas,'sep10',sep10_miles,sep10_gas,'aug10',aug10_miles,aug10_gas,'jul10',jul10_miles,jul10_gas,'jun10',jun10_miles,jun10_gas,'may10',may10_miles,may10_gas,'apr10',apr10_miles,apr10_gas,'mar10',mar10_miles,mar10_gas,'feb10',feb10_miles,feb10_gas,'jan10',jan10_miles,jan10_gas,'dec09',dec09_miles,dec09_gas,'nov09',nov09_miles,nov09_gas,'oct09',oct09_miles,oct09_gas,'sep09',sep09_miles,sep09_gas,'aug09',aug09_miles,aug09_gas,'jul09',jul09_miles,jul09_gas,'jun09',jun09_miles,jun09_gas,'may09',may09_miles,may09_gas,'apr09',apr09_miles,apr09_gas,'mar09',mar09_miles,mar09_gas,'feb09',feb09_miles,feb09_gas,'jan09',jan09_miles,jan09_gas) 
dummyalias AS data, milhas, combustivel;
```

Agora criaremos uma nova tabela contendo as médias de combustível consumido por cada caminhão usando  os dados da tabela "milhascaminhao" que acabamos de criar.
```
CREATE TABLE mediamilhas
STORED AS ORC
AS
SELECT caminhaoid, avg(mpg) mediampg
FROM milhascaminhao
GROUP BY caminhaoid;
```

Essa tabela nos mostra a média de quantas milhas os caminhões da empresa fazem para cada galão de combustível. Abaixo temos uma amostra desses dados.
```
SELECT * FROM mediamilhas LIMIT 15;
```

![MediaMilhas](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/MediaMilhas.png)


Agora vamos criar a tabela MotoristaMilhas usando a tabela MilhasCaminhao. Nela conterá o total de milhas (totmilhas) percorrida por cada motorista.
```
CREATE TABLE MotoristaMilhas
STORED AS ORC
AS
SELECT motoristaid, sum(milhas) totmilhas
FROM milhascaminhao
GROUP BY motoristaid;
```

Vamos ver uma a mostra da tabela MotoristaMilhas.
```
SELECT * FROM MotoristaMilhas LIMIT 15;
```

![MotoristaMilhas](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/MotoristaMilhas.png)

Usaremos esses resultados para calcular todos os fatores de risco dos caminhoneiros. Faremos isso através do Spark. Primeiro, vamos armazenar nossa tabela MotoristaMilhas em formato CSV no HDFS.

```
INSERT OVERWRITE DIRECTORY 'hdfs:///tmp/data/motoristamilhas'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
SELECT * from motoristamilhas;
```

Nesse momento foi exportado o arquivo 000000_0, vamos renomea-lo para motoristamilhas.csv
```hdfs dfs -mv /tmp/data/motoristamilhas/000000_0 /tmp/data/motoristamilhas/motoristamilhas.csv```

Agora finalmente vamos para a analize com Spark. O Script completo .py está [aqui](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/script/fator_de_risco.py).


Importando a biblioteca sql e instanciando SparkSession
```
from pyspark.sql import SparkSession

hiveContext = SparkSession.builder.appName("Fator de Risco sql").getOrCreate()
hiveContext.sql("SHOW TABLES").show()
```

Ainda não temos nenhuma tabela temporária dessa instância

![imagem spark0tabelas](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/spark_0_tabelas.png)


Nessa etapa vamos importar dados CSV em um DataFrame sem especificar nenhum Schema pré-definido. Em seguida, com o DataFrame criado, podemos registrar uma TempView.
```
geolocalizacaoDF = spark.read.csv('hdfs:///tmp/data/geolocalizacao/geolocalizacao.csv', header=True)
geolocalizacaoDF.createOrReplaceTempView("geolocalizacao")

hiveContext.sql("SELECT * FROM geolocalizacao LIMIT 15").show()
```
Aqui está uma amostra da tabela temporária geolocalização que acabamos de criar

![spark_geolocalizacao](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/spark_geolocalizacao.png)


Se olharmos a descrição da tabela, percebemos que todos os dados foram lançados como String, e as colunas com nomes em inglês pois não especificamos nenhum schema.
```
hiveContext.sql("DESCRIBE geolocalizacao").show()
```

![spark_desc_geolocalizacao](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/spark_desc_geolocalizacao.png)


Em contrapartida, vamos carreagar dados de um arquivo csv para um DataFrame, só que dessa vez com um schema definido. A biblioteca pyspark.sql.types nos permite definir os tipos de dados do nosso schema.
```
from pyspark.sql.types import *

motoristaMilhasSchema = StructType().add("motoristaid", "string", True).add("totmilhas", "double", True)
motoristaMilhasDF = spark.read.csv('hdfs:///tmp/data/motoristamilhas/motoristamilhas.csv', header=True, schema=motoristaMilhasSchema)
```

Agora vamos criar uma Temp View (motoristamilhas) do dataframe "motoristaMilhasDF" e ver se a tabela possui o schema que definimos.
```
motoristaMilhasDF.createOrReplaceTempView("motoristamilhas")
hiveContext.sql("DESC  motoristamilhas").show()
```

![DESC  motoristamilhas](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/spark_desc_motoristaMilhas.png)


Aqui está uma amostra de como a tabela "motoristaMilhas" ficou.
```
hiveContext.sql("SELECT * FROM motoristamilhas LIMIT 15").show()
```

![IMAGEM SPARK MOTORISTAMILHAS](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/spark_motoristamilhas.png)


#### Criando RDDs a partir de tabelas
```
geolocalizacao_temp0 = hiveContext.sql("SELECT * FROM geolocalizacao")
motoristamilhas_temp0 = hiveContext.sql("SELECT * FROM motoristamilhas")
```

E agora vamos criar tabelas globais temporárias a partir dos DataFrames que criamos
```
geolocalizacao_temp0.createOrReplaceTempView("geolocalizacao_temp0")
motoristamilhas_temp0.createOrReplaceTempView("motoristamilhas_temp0")

hiveContext.sql("SHOW TABLES").show()
```

Agora vamos fazer uma filtragem nos dados da tabela geolocalizacao para descobrir quais motoristas tiveram ocorrência de eventos não normais.
```
geolocalizacao_temp1 = hiveContext.sql("SELECT driverid, COUNT(driverid) occurance FROM geolocalizacao_temp0 WHERE event!='normal' GROUP BY driverid")

geolocalizacao_temp1.show(15)
```

Aqui está o resultado parcial de motoristas com eventos anormais.

![eventos anormais](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/geolocalizacao_temp1.png)


Registraremos esta tabela filtrada como uma tabela temporária para que as consultas SQL subsequentes possam ser aplicadas a ela.
```
geolocalizacao_temp1.createOrReplaceTempView("geolocalizacao_temp1")
```

Agora é possível vizualizar os dados da tabela através de consultas SQL e ver o total de eventos anormais para cada motorista.
```
hiveContext.sql("SELECT * FROM geolocalizacao_temp1 LIMIT 15").show()
```

![eventos anormais SQL](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/consulta%20sql%20geolocalizacao_temp1.png)

## Executar uma junção

Agora precisamso realizar uma operação de junção. A tabela geolocalizacao_temp1 possui os dados dos motoristas que possuem eventos anormais e a tabela motoristamilhas_temp0 possui o total de milhas percorridas por cada motorista. Vamos juntas esses dados que nos interessa em uma tabela só.
```
juncao = hiveContext.sql("SELECT a.driverid, a.occurance, b.totmilhas FROM geolocalizacao_temp1 a, motoristamilhas_temp0 b WHERE a.driverid=b.motoristaid")
```

Registraremos essa tabela juncao com uma tabela temporária para consultas SQL subsequentes.
```
juncao.createOrReplaceTempView("juncao")
hiveContext.sql("SHOW TABLES").show()
```
![todas as tabelas](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/totas%20tabelas%20ate%20juncao.png)

Vamos ver como ficou a tabela juncao
```
hiveContext.sql("SELECT * FROM juncao LIMIT 15").show()
```
![juncao](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/tabela%20juncao.png)

## Computanto o fator de risco para o motorista

Nessa seção associaremos um "fator de risco do motorista" para cada motorista. Esse fator de risco do motorista é o numeor de ocorrências anormais sobre o numero total de milhas percorridas pelo motorista. Ou seja, uma número alto de ocorrências anormais em um curto período de milhas percorridas, é um indicador de alto risco. Vamkso fazer isso com uma consulta SQL.
```
fator_de_risco = hiveContext.sql("SELECT driverid,occurance,totmilhas, totmilhas/occurance fatorrisco FROM juncao")
```

Vamos criar a tabela temporária para consultas SQL e vermos como ficaram os dados.
```
fator_de_risco.createOrReplaceTempView("fator_de_risco")
hiveContext.sql("SHOW TABLES").show()
```

![todas as tabelas criadas](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/todas%20as%20tabelas%20fatorderisco.png)

```
hiveContext.sql("SELECT * FROM fator_de_risco LIMIT 15").show()
```
![fator de risco](https://github.com/BrunoHarlis/FatorDeRisco/blob/main/ImagensFatorDeRisco/fator%20de%20risco%202.png)

## Salvando a tabela como CSV

Agara que encontramos o fator de risco para cada motorista, podemos salvar esse resultado como um arquivo CSV no HDFS.
```
fator_de_risco.coalesce(1).write.csv("hdfs:///tmp/data/fatorderisco")
```
