package zio.spark.sql

object Fixture {
  def readFile(path: String): Spark[DataFrame] =
    SparkSession.use(_.read.inferSchema.withHeader.withDelimiter(";").csv(path))

  val read: Spark[DataFrame] = readFile("new/src/test/resources/data.csv")

  val readEmpty: Spark[DataFrame] = readFile("new/src/test/resources/empty.csv")
}
