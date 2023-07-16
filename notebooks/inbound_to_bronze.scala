// Databricks notebook source
// MAGIC %md
// MAGIC Conferindo se os dados foram montados e se temos acesso a pasta inbound

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/inbound")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lendo os dados da camada de inbound

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// removendo as informacoes de imagens e usuarios
val dadosAnuncio = dados.drop("imagens", "usuario")
display(dadosAnuncio)

// COMMAND ----------

// criar uma coluna de id
import org.apache.spark.sql.functions.col

val dfBronze = dadosAnuncio.withColumn("id", col("anuncio.id"))
display(dfBronze)

// COMMAND ----------

// salvando o arquivo, em formato delta, na camada bronze
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
dfBronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


