package zio.spark.sql

import org.apache.spark.sql.Row

import zio.Task
import zio.spark.Fixture._
import zio.test._
import zio.test.Assertion._

object DatasetTest {

  def datasetActionsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("Dataset Actions")(
      test("Dataset should implement count correctly") {
        val write: DataFrame => Task[Long] = _.count

        val pipeline = Pipeline.buildWithoutTransformation(read)(write)

        pipeline.run.map(assert(_)(equalTo(4L)))
      },
      test("Dataset should implement collect correctly") {
        val write: DataFrame => Task[Seq[Row]] = _.collect

        val pipeline = Pipeline.buildWithoutTransformation(read)(write)

        pipeline.run.map(assert(_)(hasSize(equalTo(4))))
      },
      test("Dataset should implement head(n)/take(n) correctly") {
        val process: DataFrame => Dataset[String]       = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[Seq[String]] = _.take(2)

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo(Seq("Maria", "John"))))
      },
      test("Dataset should implement head/first correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.first

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo("Maria")))
      },
      test("Dataset should implement headOption/firstOption correctly") {
        val write: DataFrame => Task[Option[Row]] = _.firstOption

        val pipeline = Pipeline.buildWithoutTransformation(readEmpty)(write)

        pipeline.run.map(assert(_)(isNone))
      }
    )

  def datasetTransformationsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("Dataset Transformations")(
      test("Dataset should implement limit correctly") {
        val process: DataFrame => DataFrame = _.limit(2)
        val write: DataFrame => Task[Long]  = _.count

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(assert(_)(equalTo(2L)))
      },
      test("Dataset should implement as correctly") {
        val process: DataFrame => Dataset[Person]  = _.as[Person]
        val write: Dataset[Person] => Task[Person] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(equalTo(Person("Maria", 93))))
      },
      test("Dataset should implement map correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(equalTo("Maria")))
      },
      test("Dataset should implement flatMap correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].flatMap(_.name.toSeq.map(_.toString))
        val write: Dataset[String] => Task[String] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(equalTo("M")))
      }
    )

  def fromSparkSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("fromSpark")(
      test("Zio-spark can wrap spark code") {
        val job: Spark[Long] =
          fromSpark { ss =>
            val inputDf =
              ss.read
                .option("inferSchema", value = true)
                .option("header", value = true)
                .option("delimiter", ";")
                .csv("core/src/test/resources/data.csv")

            val processedDf = inputDf.limit(2)

            processedDf.count()
          }

        job.map(assert(_)(equalTo(2L)))
      }
    )

  case class Person(name: String, age: Int)
}
