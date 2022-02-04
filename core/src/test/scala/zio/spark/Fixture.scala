package zio.spark

import zio.spark.sql._

object Fixture {
  def readCsv(path: String): Spark[DataFrame] =
    SparkSession.use(_.read.inferSchema.withHeader.withDelimiter(";").csv(path))

  val read: Spark[DataFrame] = readCsv("new/src/test/resources/data.csv")

  val readEmpty: Spark[DataFrame] = readCsv("new/src/test/resources/empty.csv")

  val readLorem: Spark[Dataset[String]] = SparkSession.use(_.read.textFile("new/src/test/resources/lorem.txt"))
}
