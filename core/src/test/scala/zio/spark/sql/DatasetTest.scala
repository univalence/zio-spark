package zio.spark.sql

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.storage.StorageLevel

import zio.{Task, ZIO}
import zio.spark.helper.Fixture._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential

import scala.util.Try

object DatasetTest {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

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

  implicit class PipelineTestOps[S, O, R](pipeline: Pipeline[S, O, R]) {
    def check(f: R => Assert): ZIO[SparkSession, Throwable, Assert] = pipeline.run.map(f)
  }

  def testDataframeTransform(
      name: String
  )(transform: DataFrame => DataFrame, expectedCount: Long): ZSpec[SparkSession, Throwable] =
    test(name)(Pipeline.build(read)(transform)(_.count).check(x => assertTrue(x == expectedCount)))

  def errorSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("Dataset error handling")(
      test("Dataset still can dies with AnalysisException using 'throwAnalysisException' implicit") {

        val process: DataFrame => DataFrame = _.selectExpr("yolo")
        val job: Spark[DataFrame]           = read.map(process)

        job.exit.map(assert(_)(dies(isSubtype[AnalysisException](anything))))
      },
      testDataframeTransform("Dataset can revover from one Analysis error")(
        transform     = x => x.selectExpr("yolo").recover(_ => x),
        expectedCount = 4
      ),
      test("Dataset can recover from the first Analysis error") {
        val process: DataFrame => DataFrame = x => x.selectExpr("yolo").filter("tata = titi").recover(_ => x)
        val write: DataFrame => Task[Long]  = _.count

        val pipeline = Pipeline(read, process, write)

        pipeline.check(x => assertTrue(x == 4L))
      },
      test("Dataset can be converted from the first Analysis error") {

        val process: DataFrame => Try[DataFrame] = x => x.selectExpr("yolo").filter("tata = titi").toTry

        read.map(process).map(x => assertTrue(x.isFailure))
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

        pipeline.run.map(res => assertTrue(res == Person("Maria", 93)))
      },
      test("Dataset should implement map correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assertTrue(res == "Maria"))
      },
      test("Dataset should implement flatMap correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].flatMap(_.name.toSeq.map(_.toString))
        val write: Dataset[String] => Task[String] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assertTrue(res == "M"))
      },
      test("Dataset should implement transform correctly") {
        val subprocess: DataFrame => Dataset[String] = _.as[Person].map(_.name.drop(1))
        val process: DataFrame => Dataset[String]    = _.transform(subprocess)
        val write: Dataset[String] => Task[String]   = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assertTrue(res == "aria"))
      },
      test("Dataset should implement dropDuplicates with colnames correctly") {
        val process: DataFrame => Dataset[Person] = _.as[Person].flatMap(r => List(r, r)).dropDuplicates(Seq("name"))
        val write: Dataset[Person] => Task[Long]  = _.count

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assertTrue(res == 4L))
      },
      test("Dataset should implement distinct/dropDuplicates correctly") {
        val process: DataFrame => Dataset[Person] = _.as[Person].flatMap(r => List(r, r)).distinct
        val write: Dataset[Person] => Task[Long]  = _.count

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assertTrue(res == 4L))
      },
      test("Dataset should implement filter correctly") {
        val process: DataFrame => Dataset[Person]  = _.as[Person].filter(_.name == "Peter")
        val write: Dataset[Person] => Task[Person] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res.name)(equalTo("Peter")))
      },
      test("Dataset should implement filter correctly using expressions") {
        import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

        val process: DataFrame => Dataset[Person]  = _.as[Person].filter("name == 'Peter'")
        val write: Dataset[Person] => Task[Person] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res.name)(equalTo("Peter")))
      },
      test("Dataset should implement selectExpr correctly") {
        import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

        val process: DataFrame => Dataset[String]  = _.selectExpr("name").as[String]
        val write: Dataset[String] => Task[String] = _.head

        val pipeline = Pipeline(read, process, write)

        pipeline.run.map(res => assert(res)(equalTo("Maria")))
      }
    )

  def sqlSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("SQL Tests")(
      test("We can use sql in zio-spark") {
        val job =
          for {
            ss    <- ZIO.service[SparkSession]
            input <- read
            _     <- input.createOrReplaceTempView("people")
            df <-
              ss.sql(
                """
                  |SELECT * FROM people
                  |WHERE age BETWEEN 15 AND 30
                  |""".stripMargin
              )
            output <- df.count
          } yield output

        job.map(assert(_)(equalTo(2L)))
      }
    )

  def persistencySpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("Persistency Tests")(
      test("By default a dataset has no persistency") {
        for {
          df           <- read
          storageLevel <- df.storageLevel
          _            <- df.count
        } yield assertTrue(storageLevel == StorageLevel.NONE)
      },
      test("We can persist a DataFrame") {
        for {
          df           <- read
          persistedDf  <- df.persist
          _            <- persistedDf.count
          storageLevel <- persistedDf.storageLevel
        } yield assertTrue(storageLevel == StorageLevel.MEMORY_AND_DISK)
      },
      test("We can unpersist a DataFrame") {
        for {
          df            <- read
          persistedDf   <- df.persist
          unpersistedDf <- persistedDf.unpersist
          _             <- unpersistedDf.count
          storageLevel  <- unpersistedDf.storageLevel
        } yield assertTrue(storageLevel == StorageLevel.NONE)

      },
      test("We can unpersist a DataFrame in a blocking way") {
        for {
          df            <- read
          persistedDf   <- df.persist
          unpersistedDf <- persistedDf.unpersistBlocking
          _             <- unpersistedDf.count
          storageLevel  <- unpersistedDf.storageLevel
        } yield assertTrue(storageLevel == StorageLevel.NONE)
      }
      // The spec is using the same dataframe definition, with a mutable cache state in sparkSession.sharedState,
      // we can not run those test in parallel.
    ) @@ sequential

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
}
