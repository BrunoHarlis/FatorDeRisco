# FatorDeRisco
Análise de caso de uso usando Hadoop, Hive e Spark.


Nesse projeto será realizada uma análise de caso de uso em uma frota de caminhões. Cada caminhão possui um equipamento de registro de eventos que é enviado para um datacenter onde será realizado processos dos dados para esclarecer o fator de risco de motoristas da frota.

## Carregando os dados no HDFS

O primeiro passo será carregar os dados colhidos pelos sensorres dos caminhões no HDFS. São os arquivos:

__geolocalizacao.csv__ - Possui os dados de geolocalização dos caminhões. Contem as colunas localização caminhão, data, hora, evento, veloidade, etc.

__caminhoes.csv__ - Possui informações sobre o modelo de caminhão, motoristaid, caminhaoid e informações de quilometragem.


```
# Criar uma pasta no hdfs para armazenar os dados
hdfs dfs -mkdir /tmp/data

# Carregando os dados
hdfs dfs -put geolocalizacao.csv /tmp/data
hdfs dfs -put caminhoes.csv /tmp/data

# Definir permissão de gravação no diretorio data
hdfs dfs -chmod 777 /tmp/data
```

## Criar tabelas no Hive

Agora que temos os dados no HDFS, vamos criar tabelas e carrega-las com os dados de geolocalização e caminhões. As tabelas serão criadas no banco de dados default.

#### Criando a tabela externa ext_geolocalizacao
```
CREATE EXTENAL TABLE ext_geolocalizacao(
caminhaoid STRING, 
motoristaid STRING, 
evento STRING, 
latitude DOUBLE, 
longitude DOUBLE, 
cidade STRING,
estado STRING,
celocidade INT,
evento_ind INT,
idling_ind INT)
ROW FORMAT DELIMITED FIELD TERMINATED BY ","
STORED BY TEXTFILE
LOCATION '/tmp/data/external/geolocalizacao/geolocalizacao.csv'
TBLPROPERTIES("skip.header.line/count"="1");
```

#### Criando a tabela gerenciada geolocalizacao
```
CREATE EXTENAL TABLE geolocalizacao(
caminhaoid STRING, 
motoristaid STRING, 
evento STRING, 
latitude DOUBLE, 
longitude DOUBLE, 
cidade STRING,
estado STRING,
celocidade INT,
evento_ind INT,
idling_ind INT)
ROW FORMAT DELIMITED FIELD TERMINATED BY "|"
STORED BY ORC;
```

#### Inserindo dados da tabela ext_geolocalizacao para geolocalizacao 
```
INSERT INTO geolocalizacao 
SELECT caminhaoid,motoristaid,evento,latitude,longitude, cidade,estado,celocidade,evento_ind,idling_ind
FROM ext_geolocalizacao;
```

#### Ecluindo a tabela externa ext_geolocalizacao
```
DROP TABLE ext_geolocalizacao;
```
