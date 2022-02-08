package zio.spark.sql

import org.apache.spark.sql.Row

import zio.Task
import zio.spark.helper.Fixture._
import zio.test._
import zio.test.Assertion._

object ExtraDatasetFeatureTest {
  def spec: Spec[SparkSession, TestFailure[Any], TestSuccess] = dataFrameActionsSpec

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

        val pipeline = Pipeline.buildWithoutTransformation(readEmpty)(write)

        pipeline.run.map(assert(_)(isNone))
      },
      test("ExtraDatatasetFeature should implement summary correctly") {
        val process: DataFrame => DataFrame    = _.summary(Statistics.Count, Statistics.Max)
        val write: DataFrame => Task[Seq[Row]] = _.collect

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(hasSize(equalTo(2))))
      }
    )

  case class Person(name: String, age: Int)
}
