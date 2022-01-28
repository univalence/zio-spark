package zio.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, Row}

import zio.{Task, ZLayer}
import zio.spark.sql.SparkSession.Builder.LocalAllNodes
import zio.test._
import zio.test.Assertion._

object DatasetTest extends DefaultRunnableSpec {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] =
    SparkSession.builder
      .master(LocalAllNodes)
      .appName("zio-spark")
      .getOrCreateLayer
      .orDie

  val read: SparkSession => Task[DataFrame] =
    _.read.inferSchema.withHeader.withDelimiter(";").csv("src/test/resources/data.csv")

  val readEmpty: SparkSession => Task[DataFrame] =
    _.read.inferSchema.withHeader.withDelimiter(";").csv("src/test/resources/empty.csv")

  def spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] =
    (zDataFrameActionsSpec + zDataFrameTransformationsSpec).provideShared(session)

  def zDataFrameActionsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("ZDataset Actions")(
      test("ZDataset should implement count correctly") {
        val write: DataFrame => Task[Long] = _.count

        val pipeline = Pipeline.buildWithoutProcessing(read)(write)

        pipeline.run.map(assert(_)(equalTo(4L)))
      },
      test("ZDataset should implement collect correctly") {
        val write: DataFrame => Task[List[Row]] = _.collect

        val pipeline = Pipeline.buildWithoutProcessing(read)(write)

        pipeline.run.map(assert(_)(hasSize(equalTo(4))))
      },
      test("ZDataset should implement head(n)/take(n) correctly") {
        val process: DataFrame => Dataset[String]        = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[List[String]] = _.take(2)

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo(List("Maria", "John"))))
      },
      test("ZDataset should implement head/first correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.first

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo("Maria")))
      },
      test("ZDataset should implement headOption/firstOption correctly") {
        val write: DataFrame => Task[Option[Row]] = _.firstOption

        val pipeline = Pipeline.buildWithoutProcessing(readEmpty)(write)

        pipeline.run.map(assert(_)(isNone))
      },
      test("ZDataset should implement tail(n)/takeRight(n) correctly") {
        val process: DataFrame => Dataset[String]        = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[List[String]] = _.takeRight(2)

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo(List("Peter", "Cassandra"))))
      },
      test("ZDataset should implement tail/last correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.last

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo("Cassandra")))
      },
      test("ZDataset should implement tailOption/lastOption correctly") {
        val write: DataFrame => Task[Option[Row]] = _.lastOption

        val pipeline = Pipeline.buildWithoutProcessing(readEmpty)(write)

        pipeline.run.map(assert(_)(isNone))
      }
    )

  def zDataFrameTransformationsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("ZDataset Transformations")(
      test("ZDataset should implement limit correctly") {
        val process: DataFrame => DataFrame = _.limit(2)
        val write: DataFrame => Task[Long]  = _.count

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(assert(_)(equalTo(2L)))
      },
      test("ZDataset should implement as correctly") {
        val process: DataFrame => Dataset[Person]        = _.as[Person]
        val write: Dataset[Person] => Task[List[Person]] = _.collect

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res.headOption)(isSome(equalTo(Person("Maria", 93)))))
      },
      test("ZDataset should implement map correctly") {
        val process: DataFrame => Dataset[String]        = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[List[String]] = _.collect

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res.headOption)(isSome(equalTo("Maria"))))
      },
      test("ZDataset should implement flatMap correctly") {
        val process: DataFrame => Dataset[String]        = _.as[Person].flatMap(_.name.toList.map(_.toString))
        val write: Dataset[String] => Task[List[String]] = _.collect

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res.headOption)(isSome(equalTo("M"))))
      }
    )

  implicit val personEncoder: Encoder[Person] = Encoders.product[Person]
  implicit val stringEncoder: Encoder[String] = Encoders.STRING

  case class Person(name: String, age: Int)
}
