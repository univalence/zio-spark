package zio.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row

import zio.{Task, ZIO, ZLayer}
import zio.spark.parameter._
import zio.test._
import zio.test.Assertion._

object ExtraDatatasetFeatureTest extends DefaultRunnableSpec {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] =
    SparkSession.builder
      .master(localAllNodes)
      .appName("zio-spark")
      .getOrCreateLayer
      .orDie

  val read: Spark[DataFrame] =
    ZIO
      .service[SparkSession]
      .flatMap(_.read.inferSchema.withHeader.withDelimiter(";").csv("new/src/test/resources/data.csv"))

  val readEmpty: Spark[DataFrame] =
    ZIO
      .service[SparkSession]
      .flatMap(_.read.inferSchema.withHeader.withDelimiter(";").csv("new/src/test/resources/empty.csv"))

  def spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] = dataFrameActionsSpec.provideShared(session)

  def dataFrameActionsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("ExtraDatatasetFeature Actions")(
      test("ExtraDatatasetFeature should implement tail(n)/takeRight(n) correctly") {
        val process: DataFrame => Dataset[String]       = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[Seq[String]] = _.takeRight(2)

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo(Seq("Peter", "Cassandra"))))
      },
      test("ExtraDatatasetFeature should implement tail/last correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.last

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo("Cassandra")))
      },
      test("ExtraDatatasetFeature should implement tailOption/lastOption correctly") {
        val write: DataFrame => Task[Option[Row]] = _.lastOption

        val pipeline = Pipeline.buildWithoutProcessing(readEmpty)(write)

        pipeline.run.map(assert(_)(isNone))
      }
    )

  case class Person(name: String, age: Int)
}
