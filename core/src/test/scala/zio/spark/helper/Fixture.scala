package zio.spark.helper

import zio.spark.rdd._
import zio.spark.sql._
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

object Fixture {
  final case class Person(name: String, age: Int)

  val resourcesPath: String = "core/src/test/resources"

  def readCsv(path: String): Spark[DataFrame] =
    SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv(s"$resourcesPath/$path")

  val read: Spark[DataFrame] = readCsv("data.csv")

  val readEmpty: Spark[DataFrame] = readCsv("empty.csv")

  val readLorem: Spark[Dataset[String]] = SparkSession.read.textFile(s"$resourcesPath/lorem.txt")

  val readRDD: Spark[RDD[Person]] = read.map(_.as[Person].rdd)
}
