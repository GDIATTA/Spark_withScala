// Databricks notebook source
// MAGIC %md
// MAGIC # Dans ce TP vous allez éxaminer le dataset Titanic

// COMMAND ----------

// MAGIC %md
// MAGIC Faites le choses préliminaires

// COMMAND ----------

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

// COMMAND ----------

val spark = SparkSession.builder
  .master("local[*]")  // Set the master URL to run Spark locally with all available cores
  .appName("Titanic")  // Set a name for your application
  .getOrCreate()

// COMMAND ----------

println(spark)

// COMMAND ----------

val root = "/FileStore/tables"
val path = root + "/titanic.csv"  // Assuming 'root' is defined previously

// COMMAND ----------

val df = spark.read
  .option("header", "true")  // Use first line of all files as header
  .option("inferSchema", "true")  // Automatically infer data types
  .csv(path)

df.printSchema()  // Print the schema of the DataFrame
df.show(5)  // Show the first 5 rows of the DataFrame

// COMMAND ----------

val rowCount = df.count()
println(rowCount)

// COMMAND ----------

df.describe().show(false)

// COMMAND ----------

df.dtypes

// COMMAND ----------

df.columns

// COMMAND ----------

df.columns.length

// COMMAND ----------

// MAGIC %md
// MAGIC Combinen des passagers il y a dans le DataFrame ?

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Tranformez le Spark DataFrame en Pandas DataFrame et inspectez-le  
// MAGIC #### Attention : ne faites pas ça avec des gros jeux des données.

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = df.schema
val rows: Array[Row] = df.collect()
val dfP = spark.createDataFrame(rows.toList, schema)

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = df.schema
val rows: Array[Row] = df.collect()

// COMMAND ----------

import org.apache.spark.sql.functions._

df.show

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez et comptez le nombre de passagers de sex feminin; essayes plusieurs syntaxes

// COMMAND ----------

val femaleCount = df.filter($"Sex" === "female").count()

// COMMAND ----------

// MAGIC %md
// MAGIC Créee un SQL temporary view et sélectionnez Age et Sex ou le Sex est féminin et l'Age > 20

// COMMAND ----------

df.createOrReplaceTempView("dfS")

// COMMAND ----------

val result = spark.sql("SELECT Age, Sex FROM dfS WHERE Sex = 'female' AND Age > 20")
result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Compter les nombre de passgers de sexe féminin et don l'âge est > 20 ans

// COMMAND ----------

val rowCount = spark.sql("SELECT COUNT(*) FROM dfS WHERE Sex = 'female' AND Age > 20").collect()(0)(0).asInstanceOf[Long]

// COMMAND ----------

// MAGIC %md
// MAGIC Compter le nombre de passagers dont l'âge n'est pas null

// COMMAND ----------

val rowCount = spark.sql("SELECT COUNT(*) FROM dfS WHERE Age != 0").collect()(0)(0).asInstanceOf[Long]

// COMMAND ----------

// MAGIC %md
// MAGIC Calculez l'âge moyen, mimimum, maximum et rénommez les colonnes

// COMMAND ----------

val result = spark.sql("SELECT AVG(Age), MIN(Age), MAX(Age) FROM dfS")
result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ajoutez une colonne donnant l'age en mois et donnez un nom significatif

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfWithAgeEnMois = df.withColumn("Age_en_mois", col("Age") * 12)
dfWithAgeEnMois.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Transformez la colonne Age de façon que il donne l'âge en mois et donnez un nom significatif

// COMMAND ----------

val dfWithUpdatedAge = df.withColumn("Age", col("Age") * 12)
dfWithUpdatedAge.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez l'âge moyen des passagers groupés par classe et port de départ et triez la sortie par classe et port de départ  
// MAGIC Trouvez les bonnes colonnes en examinant le schéma

// COMMAND ----------

df.printSchema()
df.show(3, truncate=false)

// COMMAND ----------

val result = df.groupBy("Pclass", "Embarked").agg(mean("Age").alias("avg_age"))
result.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Crées un nouveau DataFrame ou pour les passagers dont l'âge est null dans le DataFrame d'origine la moyenne est imputée
// MAGIC Comptez le nombre des nulls dans le deux DataFrames

// COMMAND ----------

val meanAge = df.agg(mean(col("Age"))).head().getDouble(0)

// COMMAND ----------

val dfIm = df.na.fill(meanAge, Seq("Age"))
dfIm.show()

// COMMAND ----------

val nullAgeCount = df.filter(df("Age").isNull).count()

// COMMAND ----------

val nullAgeCount = dfIm.filter(dfIm("Age").isNull).count()

// COMMAND ----------

// MAGIC %md
// MAGIC Combien des passagers de sexe masculin et féminin il y a dans le DataFrame ?

// COMMAND ----------

val result = df.groupBy("Sex").count()
result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la probabilité de survie des passagers de sexe masculin et féminin  
// MAGIC Hint: la fonction count() utilisée pour visualiser les DataFrame revoie un entier dont la valeur peut être utilisée pour calculer la probabilité

// COMMAND ----------

val survivedMaleCount = df.filter($"Sex" === "male" && $"Survived" === 1).count().toInt
val totalMaleCount = df.filter($"Sex" === "male").count().toInt

val ratio = survivedMaleCount.toDouble / totalMaleCount.toDouble

// COMMAND ----------

val survivedFemaleCount = df.filter($"Sex" === "female" && $"Survived" === 1).count().toInt
val totalFemaleCount = df.filter($"Sex" === "female").count().toInt

val ratio = survivedFemaleCount.toDouble / totalFemaleCount.toDouble

// COMMAND ----------

val root = "/FileStore/tables"
val path1 = root + "/titanic.csv"  // Assuming 'root' is defined previously
val path2 = root + "/titanic_parquet"

// COMMAND ----------

df.write
  .mode("overwrite")
  .csv(path1)

// COMMAND ----------

df.write
  .mode("overwrite")
  .parquet(path2)

// COMMAND ----------


