# FatorDeRisco
Análise de caso de uso usando Hadoop, Hive e Spark.


Nesse projeto será realizada uma análise de caso de uso em uma frota de caminhões. Cada caminhão possui um equipamento de registro de eventos que é enviado para um datacenter onde será realizado processos dos dados para esclarecer o fator de risco de motoristas da frota.

## Carregando os dados no HDFS

O primeiro passo será carregar os dados colhidos pelos sensorres dos caminhões no HDFS. São os arquivos:

__geolocalizacao.csv__ - Possui os dados de geolocalização dos caminhões. Contem as colunas localização caminhão, data, hora, evento, veloidade, etc.

__caminhoes.csv__ - Possui informações sobre o modelo de caminhão, motoristaid, caminhaoid e informações de milhagem.

#### Criar uma pasta no hdfs para armazenar os dados
```
hdfs dfs -mkdir -p /tmp/data/external/{geolocalizacao,caminhoes}
```

#### Carregando os dados
```
hdfs dfs -put geolocalizacao.csv /tmp/data/external/geolocalizacao
hdfs dfs -put caminhoes.csv /tmp/data/external/caminhoes
```

#### Definir permissão de gravação no diretório external
```
hdfs dfs -chmod 777 /tmp/data/external
```

## Criar tabelas no Hive

Agora que temos os dados no HDFS, vamos criar tabelas e carrega-las com os dados de geolocalização e caminhões. As tabelas serão criadas no banco de dados default.

#### Criando a tabela externa "ext_geolocalizacao"
```
CREATE EXTERNAL TABLE ext_geolocalizacao(
truckid STRING,
driverid STRING, 
event STRING, 
latitude DOUBLE, 
longitude DOUBLE, 
city STRING,
state STRING,
velocity INT,
event_ind INT,
idling_ind INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '/tmp/data/external/geolocalizacao'
TBLPROPERTIES("skip.header.line.count"="1");
```

#### Criando a tabela gerenciada "geolocalizacao"
```
CREATE TABLE geolocalizacao(
truckid STRING, 
driverid STRING, 
event STRING, 
latitude DOUBLE, 
longitude DOUBLE, 
city STRING,
state STRING,
velocity INT,
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
driverid STRING,truckid STRING,model STRING,jun13_miles INT,jun13_gas INT,may13_miles INT,may13_gas INT,apr13_miles INT,apr13_gas INT,mar13_miles INT,mar13_gas INT,feb13_miles INT,feb13_gas INT,jan13_miles INT,jan13_gas INT,dec12_miles INT,dec12_gas INT,nov12_miles INT,nov12_gas INT,oct12_miles INT,oct12_gas INT,sep12_miles INT,sep12_gas INT,aug12_miles INT,aug12_gas INT,jul12_miles INT,jul12_gas INT,jun12_miles INT,jun12_gas INT,may12_miles INT,may12_gas INT,apr12_miles INT,apr12_gas INT,mar12_miles INT,mar12_gas INT,feb12_miles INT,feb12_gas INT,jan12_miles INT,jan12_gas INT,dec11_miles INT,dec11_gas INT,nov11_miles INT,nov11_gas INT,oct11_miles INT,oct11_gas INT,sep11_miles INT,sep11_gas INT,aug11_miles INT,aug11_gas INT,jul11_miles INT,jul11_gas INT,jun11_miles INT,jun11_gas INT,may11_miles INT,may11_gas INT,apr11_miles INT,apr11_gas INT,mar11_miles INT,mar11_gas INT,feb11_miles INT,feb11_gas INT,jan11_miles INT,jan11_gas INT,dec10_miles INT,dec10_gas INT,nov10_miles INT,nov10_gas INT,oct10_miles INT,oct10_gas INT,sep10_miles INT,sep10_gas INT,aug10_miles INT,aug10_gas INT,jul10_miles INT,jul10_gas INT,jun10_miles INT,jun10_gas INT,may10_miles INT,may10_gas INT,apr10_miles INT,apr10_gas INT,mar10_miles INT,mar10_gas INT,feb10_miles INT,feb10_gas INT,jan10_miles INT,jan10_gas INT,dec09_miles INT,dec09_gas INT,nov09_miles INT,nov09_gas INT,oct09_miles INT,oct09_gas INT,sep09_miles INT,sep09_gas INT,aug09_miles INT,aug09_gas INT,jul09_miles INT,jul09_gas INT,jun09_miles INT,jun09_gas INT,may09_miles INT,may09_gas INT,apr09_miles INT,apr09_gas INT,mar09_miles INT,mar09_gas INT,feb09_miles INT,feb09_gas INT,jan09_miles INT,jan09_gas INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/tmp/data/external/caminhoes'
TBLPROPERTIES ("skip.header.line.count"="1");
```


#### Criando a tabela gerenciada "caminhoes"
```
CREATE TABLE caminhoes(
driverid STRING,truckid STRING,model STRING,jun13_miles INT,jun13_gas INT,may13_miles INT,may13_gas INT,apr13_miles INT,apr13_gas INT,mar13_miles INT,mar13_gas INT,feb13_miles INT,feb13_gas INT,jan13_miles INT,jan13_gas INT,dec12_miles INT,dec12_gas INT,nov12_miles INT,nov12_gas INT,oct12_miles INT,oct12_gas INT,sep12_miles INT,sep12_gas INT,aug12_miles INT,aug12_gas INT,jul12_miles INT,jul12_gas INT,jun12_miles INT,jun12_gas INT,may12_miles INT,may12_gas INT,apr12_miles INT,apr12_gas INT,mar12_miles INT,mar12_gas INT,feb12_miles INT,feb12_gas INT,jan12_miles INT,jan12_gas INT,dec11_miles INT,dec11_gas INT,nov11_miles INT,nov11_gas INT,oct11_miles INT,oct11_gas INT,sep11_miles INT,sep11_gas INT,aug11_miles INT,aug11_gas INT,jul11_miles INT,jul11_gas INT,jun11_miles INT,jun11_gas INT,may11_miles INT,may11_gas INT,apr11_miles INT,apr11_gas INT,mar11_miles INT,mar11_gas INT,feb11_miles INT,feb11_gas INT,jan11_miles INT,jan11_gas INT,dec10_miles INT,dec10_gas INT,nov10_miles INT,nov10_gas INT,oct10_miles INT,oct10_gas INT,sep10_miles INT,sep10_gas INT,aug10_miles INT,aug10_gas INT,jul10_miles INT,jul10_gas INT,jun10_miles INT,jun10_gas INT,may10_miles INT,may10_gas INT,apr10_miles INT,apr10_gas INT,mar10_miles INT,mar10_gas INT,feb10_miles INT,feb10_gas INT,jan10_miles INT,jan10_gas INT,dec09_miles INT,dec09_gas INT,nov09_miles INT,nov09_gas INT,oct09_miles INT,oct09_gas INT,sep09_miles INT,sep09_gas INT,aug09_miles INT,aug09_gas INT,jul09_miles INT,jul09_gas INT,jun09_miles INT,jun09_gas INT,may09_miles INT,may09_gas INT,apr09_miles INT,apr09_gas INT,mar09_miles INT,mar09_gas INT,feb09_miles INT,feb09_gas INT,jan09_miles INT,jan09_gas INT)
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

Vamos começar calculando a quantidade de milhas por galão que cada caminhão consome. Começaremos com nossa tabela de dados de caminhões. Precisamos somar todas as milhas e colunas de combustível por caminhão. O Hive tem uma série de funções que podem ser usadas para reformatar uma tabela. A palavra-chave LATERAL VIEW é como invocamos as coisas. A função stack() nos permite reestruturar os dados em 3 colunas rotuladas rdate, gas e mile (ex: 'june13', june13_miles, june13_gas) que perfazem um máximo de 54 linhas. Escolhemos truckid, driverid, rdate, miles, gas de nossa tabela original e adicionamos uma coluna chamada mpg que calculada a milhagem média (miles/gas).

```
CREATE TABLE milhascaminhao 
STORED AS ORC 
AS 
SELECT truckid, driverid, rdate, miles, gas, miles/gas mpg 
FROM caminhoes LATERAL VIEW stack(
54,'jun13',jun13_miles,jun13_gas,'may13',may13_miles,may13_gas,'apr13',apr13_miles,apr13_gas,'mar13',mar13_miles,mar13_gas,'feb13',feb13_miles,feb13_gas,'jan13',jan13_miles,jan13_gas,'dec12',dec12_miles,dec12_gas,'nov12',nov12_miles,nov12_gas,'oct12',oct12_miles,oct12_gas,'sep12',sep12_miles,sep12_gas,'aug12',aug12_miles,aug12_gas,'jul12',jul12_miles,jul12_gas,'jun12',jun12_miles,jun12_gas,'may12',may12_miles,may12_gas,'apr12',apr12_miles,apr12_gas,'mar12',mar12_miles,mar12_gas,'feb12',feb12_miles,feb12_gas,'jan12',jan12_miles,jan12_gas,'dec11',dec11_miles,dec11_gas,'nov11',nov11_miles,nov11_gas,'oct11',oct11_miles,oct11_gas,'sep11',sep11_miles,sep11_gas,'aug11',aug11_miles,aug11_gas,'jul11',jul11_miles,jul11_gas,'jun11',jun11_miles,jun11_gas,'may11',may11_miles,may11_gas,'apr11',apr11_miles,apr11_gas,'mar11',mar11_miles,mar11_gas,'feb11',feb11_miles,feb11_gas,'jan11',jan11_miles,jan11_gas,'dec10',dec10_miles,dec10_gas,'nov10',nov10_miles,nov10_gas,'oct10',oct10_miles,oct10_gas,'sep10',sep10_miles,sep10_gas,'aug10',aug10_miles,aug10_gas,'jul10',jul10_miles,jul10_gas,'jun10',jun10_miles,jun10_gas,'may10',may10_miles,may10_gas,'apr10',apr10_miles,apr10_gas,'mar10',mar10_miles,mar10_gas,'feb10',feb10_miles,feb10_gas,'jan10',jan10_miles,jan10_gas,'dec09',dec09_miles,dec09_gas,'nov09',nov09_miles,nov09_gas,'oct09',oct09_miles,oct09_gas,'sep09',sep09_miles,sep09_gas,'aug09',aug09_miles,aug09_gas,'jul09',jul09_miles,jul09_gas,'jun09',jun09_miles,jun09_gas,'may09',may09_miles,may09_gas,'apr09',apr09_miles,apr09_gas,'mar09',mar09_miles,mar09_gas,'feb09',feb09_miles,feb09_gas,'jan09',jan09_miles,jan09_gas) 
dummyalias AS rdate, miles, gas;
```

Agora criaremos uma nova tabela contendo as médias de combustível consumido por cada caminhão usando  os dados da tabela "milhascaminhao" que acabamos de criar.
```
CREATE TABLE mediamilhagem
STORED AS ORC
AS
SELECT truckid, avg(mpg) mediampg
FROM milhascaminhao
GROUP BY truckid;
```

Essa tabela nos mostra a média de quantas milhas os caminhões da empresa fazem para cada galão de combustível. Abaixo temos uma amostra desses dados.

```SELECT * FROM mediamilhagem LIMIT 15```

(ADD IMAGEM DE AMOSTRA DA TABELA meamilhagem)

Agora vamos criar a tabela MotoristaMilhagem usando a tabela MilhasCaminhao. Nela conterá o total de milhas (totmiles) percorrida or cada motorista.


```
CREATE TABLE MotoristaMilhagem
STORED AS ORC
AS
SELECT driverid, sum(miles) totmiles
FROM milhascaminhao
GROUP BY driverid;
```

Vamos ve ruma a mostra da tabela MotoristaMilhagem.

(IMAGEM COM AMOSTRA DE MotoristaMilhagem)

