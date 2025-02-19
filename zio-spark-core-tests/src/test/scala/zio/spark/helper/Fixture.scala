package zio.spark.helper

import zio.spark.rdd._
import zio.spark.sql._
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
import zio.spark.sql.implicits._

import java.nio.file.Paths

object Fixture {
  import scala3encoders.given // scalafix:ok

  final case class Person(name: String, age: Long)

  def readCsv(path: String): SIO[DataFrame] = SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv(path)

  def resourcesPath(fileName: String): String =
    Paths.get(this.getClass.getClassLoader.getResource(fileName).toURI).toAbsolutePath.toString // scalafix:ok

  val targetsPath: String = "zio-spark-core-tests/target/test"

  val read: SIO[DataFrame] = readCsv(resourcesPath("data-csv"))

  val readEmpty: SIO[DataFrame] = readCsv(resourcesPath("empty.csv"))

  val readLorem: SIO[Dataset[String]] = SparkSession.read.textFile(resourcesPath("lorem.txt"))

  val readRDD: SIO[RDD[Person]] = read.map(_.as[Person].rdd)
}
