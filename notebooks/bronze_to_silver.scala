// Databricks notebook source
// MAGIC %md
// MAGIC Conferindo se os dados foram montados e se temos acesso a pasta bronze

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lendo os dados da camada Bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val dados = spark.read.format("delta").load(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transformando os campos do JSON em colunas

// COMMAND ----------

// visualizando a coluna anuncio
display(dados.select("anuncio.*"))

// COMMAND ----------

// visualizando a coluna anuncio, expandindo a coluna endereco
display(dados.select("anuncio.*", "anuncio.endereco.*"))

// COMMAND ----------

val dadosDetalhados = dados.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(dadosDetalhados)

// COMMAND ----------

// excluindo colunas com caracteristicas dos imoveis e a coluna de endereco
val dfSilver = dadosDetalhados.drop("caracteristicas", "endereco")
display(dfSilver)

// COMMAND ----------

// salvando na camada silver
val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
dfSilver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------


