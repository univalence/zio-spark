package zio.spark.helper

import zio.spark.sql.{DataFrame, Dataset, Spark, SparkSession}

object Fixture {
  final case class Person(name: String, age: Int)

  def readCsv(path: String): Spark[DataFrame] = SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv(path)

  val resourcesPath = "core/src/test/resources"

  val read: Spark[DataFrame] = readCsv(s"$resourcesPath/data.csv")

  val readEmpty: Spark[DataFrame] = readCsv(s"$resourcesPath/empty.csv")

  val readLorem: Spark[Dataset[String]] = SparkSession.read.textFile(s"$resourcesPath/lorem.txt")
}
