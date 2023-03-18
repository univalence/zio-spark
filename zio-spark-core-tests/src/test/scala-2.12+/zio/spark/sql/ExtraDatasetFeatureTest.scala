package zio.spark.sql

import org.apache.spark.sql.Row

import zio.Task
import zio.spark.helper.Fixture._
import zio.spark.test._
import zio.test._
import zio.test.TestAspect._

object ExtraDatasetFeatureTest extends ZIOSparkSpecDefault {
  import scala3encoders.given // scalafix:ok

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
  import zio.spark.sql.implicits._

  def spec: Spec[SparkSession, Any] = suite("ExtraDatasetFeatureTest tests")(dataFrameActionsSpec)

  def dataFrameActionsSpec: Spec[SparkSession, Any] =
    suite("ExtraDatatasetFeature actions")(
      test("ExtraDatatasetFeature should implement tail(n)/takeRight(n) correctly") {
        val process: DataFrame => Dataset[String]       = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[Seq[String]] = _.takeRight(2)

        read.map(process).flatMap(write).map(output => assertTrue(output == Seq("Peter", "Cassandra")))
      },
      test("ExtraDatatasetFeature should implement tail/last correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.last

        read.map(process).flatMap(write).map(output => assertTrue(output == "Cassandra"))
      },
      test("ExtraDatatasetFeature should implement tailOption/lastOption correctly") {
        val write: DataFrame => Task[Option[Row]] = _.lastOption

        readEmpty.flatMap(write).map(output => assertTrue(output.isEmpty))
      },
      test("ExtraDatatasetFeature should implement summary correctly") {
        val process: DataFrame => DataFrame    = _.summary(Statistics.Count, Statistics.Max)
        val write: DataFrame => Task[Seq[Row]] = _.collect

        read.map(process).flatMap(write).map(res => assertTrue(res.length == 2))
      },
      test("Dataset should implement explain correctly") {
        for {
          df <- read
          transformedDf = df.withColumnRenamed("name", "fullname").filter($"age" > 30)
          _      <- transformedDf.explain("simple")
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation.contains("== Physical Plan =="))
      } @@ silent
    )

  final case class Person(name: String, age: Int)
}
