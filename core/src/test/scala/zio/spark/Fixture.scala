package zio.spark

import zio.spark.sql._

object Fixture {
  def readCsv(path: String): Spark[DataFrame] = SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv(path)

  val read: Spark[DataFrame] = readCsv("core/src/test/resources/data.csv")

  val readEmpty: Spark[DataFrame] = readCsv("core/src/test/resources/empty.csv")

  val readLorem: Spark[Dataset[String]] = SparkSession.read.textFile("core/src/test/resources/lorem.txt")
}
