package zio.spark.sql

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.storage.StorageLevel

import zio._
import zio.spark.helper.Fixture._
import zio.spark.test._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import scala.util.Try

object DatasetSpec extends ZIOSparkSpecDefault {

  import scala3encoders.given // scalafix:ok

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
  import zio.spark.sql.implicits._

  implicit class SparkTestOps[O](effect: SIO[O]) {
    def check(f: O => TestResult): SIO[TestResult] = effect.map(f).orDie
  }

  def datasetActionsSpec: Spec[SparkSession, Throwable] =
    suite("Dataset Actions")(
      test("We can create an empty dataset from sparksession") {
        for {
          df    <- SparkSession.emptyDataset[Person]
          count <- df.count
        } yield assertTrue(count == 0L)
      },
      test("Dataset should implement count correctly") {
        val write: DataFrame => Task[Long] = _.count

        read.flatMap(write).check(res => assertTrue(res == 4L))
      },
      test("Dataset should implement collect correctly") {
        val write: DataFrame => Task[Seq[Row]] = _.collect

        read.flatMap(write).check(res => assertTrue(res.length == 4))
      },
      test("Dataset should implement head(n)/take(n) correctly") {
        val process: DataFrame => Dataset[String]       = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[Seq[String]] = _.take(2)

        read.map(process).flatMap(write).check(res => assertTrue(res == Seq("Maria", "John")))
      },
      test("Dataset should implement head/first correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.first

        read.map(process).flatMap(write).check(res => assertTrue(res == "Maria"))
      },
      test("Dataset should implement headOption/firstOption correctly") {
        val write: DataFrame => Task[Option[Row]] = _.firstOption

        readEmpty.flatMap(write).check(res => assertTrue(res.isEmpty))
      },
      test("Dataset should implement show correctly") {
        val result =
          """+---------+---+
            >|     name|age|
            >+---------+---+
            >|    Maria| 93|
            >|     John| 24|
            >|    Peter| 19|
            >|Cassandra| 46|
            >+---------+---+
            >
            >""".stripMargin('>')
        for {
          df     <- read
          _      <- df.show
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation == result)
      } @@ silent,
      test("Dataset should implement show with truncate correctly") {
        val result =
          """+---------+---+
            >|name     |age|
            >+---------+---+
            >|Maria    |93 |
            >|John     |24 |
            >|Peter    |19 |
            >|Cassandra|46 |
            >+---------+---+
            >
            >""".stripMargin('>')
        for {
          df     <- read
          _      <- df.show(truncate = false)
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation == result)
      } @@ silent,
      test("Dataset should implement printSchema correctly") {
        val result =
          """root
            > |-- name: string (nullable = true)
            > |-- age: integer (nullable = true)""".stripMargin('>')

        for {
          df     <- read
          _      <- df.printSchema
          output <- TestConsole.output
          representation = output.mkString("\n")
        } yield assertTrue(representation.contains(result))
      } @@ silent
    )

  def errorSpec: Spec[SparkSession, Throwable] =
    suite("Dataset error handling")(
      test("Dataset still can dies with AnalysisException using 'throwAnalysisException' implicit") {
        val process: DataFrame => DataFrame = _.selectExpr("yolo")
        val job: SIO[DataFrame]             = read.map(process)

        job.exit.map(assert(_)(dies(isSubtype[AnalysisException](anything))))
      },
      test("Dataset can revover from one Analysis error") {
        val process: DataFrame => DataFrame = x => x.selectExpr("yolo").recover(_ => x)

        read.map(process).flatMap(_.count).check(res => assertTrue(res == 4L))
      },
      test("Dataset can recover from the first Analysis error") {
        val process: DataFrame => DataFrame = x => x.selectExpr("yolo").filter("tata = titi").recover(_ => x)
        val write: DataFrame => Task[Long]  = _.count

        read.map(process).flatMap(write).check(x => assertTrue(x == 4L))
      },
      test("Dataset can be converted from the first Analysis error") {
        val process: DataFrame => Try[DataFrame] = x => x.selectExpr("yolo").filter("tata = titi").toTry

        read.map(process).map(x => assertTrue(x.isFailure))
      }
    )

  def datasetTransformationsSpec: Spec[SparkSession, Throwable] =
    suite("Dataset Transformations")(
      test("Dataset should implement limit correctly") {
        val process: DataFrame => DataFrame = _.limit(2)
        val write: DataFrame => Task[Long]  = _.count

        read.map(process).flatMap(write).map(output => assertTrue(output == 2L))
      },
      test("Dataset should implement as correctly") {
        val process: DataFrame => Dataset[Person]  = _.as[Person]
        val write: Dataset[Person] => Task[Person] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res == Person("Maria", 93)))
      },
      test("Dataset should implement map correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].map(_.name)
        val write: Dataset[String] => Task[String] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res == "Maria"))
      },
      test("Dataset should implement flatMap correctly") {
        val process: DataFrame => Dataset[String]  = _.as[Person].flatMap(_.name.toSeq.map(_.toString))
        val write: Dataset[String] => Task[String] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res == "M"))
      },
      test("Dataset should implement transform correctly") {
        val subprocess: DataFrame => Dataset[String] = _.as[Person].map(_.name.drop(1))
        val process: DataFrame => Dataset[String]    = _.transform(subprocess)
        val write: Dataset[String] => Task[String]   = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res == "aria"))
      },
      test("Dataset should implement dropDuplicates with colnames correctly") {
        val process: DataFrame => Dataset[Person] = _.as[Person].flatMap(r => List(r, r)).dropDuplicates("name")
        val write: Dataset[Person] => Task[Long]  = _.count

        read.map(process).flatMap(write).map(res => assertTrue(res == 4L))
      },
      test("Dataset should implement distinct/dropDuplicates all correctly") {
        val process: DataFrame => Dataset[Person] = _.as[Person].flatMap(r => List(r, r)).distinct
        val write: Dataset[Person] => Task[Long]  = _.count

        read.map(process).flatMap(write).map(res => assertTrue(res == 4L))
      },
      test("Dataset should implement filter/where correctly") {
        val process: DataFrame => Dataset[Person]  = _.as[Person].where(_.name == "Peter")
        val write: Dataset[Person] => Task[Person] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res.name == "Peter"))
      },
      test("Dataset should implement filter/where correctly using sql") {
        import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

        val process: DataFrame => Dataset[Person]  = _.as[Person].where("name == 'Peter'")
        val write: Dataset[Person] => Task[Person] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res.name == "Peter"))
      },
      test("Dataset should implement filter/where correctly using column expression") {
        import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

        val process: DataFrame => Dataset[Person]  = _.as[Person].where($"name" === "Peter")
        val write: Dataset[Person] => Task[Person] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res.name == "Peter"))
      },
      test("Dataset should implement selectExpr correctly") {
        import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

        val process: DataFrame => Dataset[String]  = _.selectExpr("name").as[String]
        val write: Dataset[String] => Task[String] = _.head

        read.map(process).flatMap(write).map(res => assertTrue(res == "Maria"))
      },
      test("Dataset should implement drop using colname correctly") {
        for {
          df <- read
          dfWithName = df.drop("age").as[String]
          output <- dfWithName.head
        } yield assertTrue(output == "Maria")
      },
      test("Dataset should implement drop using column correctly") {
        for {
          df <- read
          dfWithName = df.drop($"age").as[String]
          output <- dfWithName.head
        } yield assertTrue(output == "Maria")
      },
      test("Dataset should implement withColumnRenamed correctly") {
        for {
          df <- read
          colnames = df.withColumnRenamed("name", "firstname").columns
        } yield assertTrue(colnames == Seq("firstname", "age"))
      },
      test("Dataset should implement withColumn correctly") {
        for {
          df <- read
          colnames = df.withColumn("firstname", $"name").columns
        } yield assertTrue(colnames == Seq("name", "age", "firstname"))
      },
      test("Dataset should implement repartition correctly") {
        for {
          df <- read
          transformedDf = df.repartition(10)
        } yield assertTrue(transformedDf.rdd.partitions.length == 10)
      },
      test("Dataset should implement coalesce correctly") {
        for {
          df <- read
          transformedDf = df.repartition(10).coalesce(2)
        } yield assertTrue(transformedDf.rdd.partitions.length == 2)
      } @@ scala211(ignore)
    )

  def sqlSpec: Spec[SparkSession, Any] =
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

        job.map(result => assertTrue(result == 2L))
      }
    )

  def persistencySpec: Spec[SparkSession, Throwable] =
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

  def fromSparkSpec: Spec[SparkSession, Throwable] =
    suite("fromSpark")(
      test("Zio-spark can wrap spark code") {
        val job: SIO[Long] =
          fromSpark { ss =>
            val inputDf =
              ss.read
                .option("inferSchema", value = true)
                .option("header", value = true)
                .option("delimiter", ";")
                .csv(s"$resourcesPath/data-csv")

            val processedDf = inputDf.limit(2)

            processedDf.count()
          }

        job.map(output => assertTrue(output == 2L))
      }
    )

  override def spec: Spec[SparkSession, Any] =
    suite("Dataset tests")(
      datasetActionsSpec,
      datasetTransformationsSpec,
      sqlSpec,
      persistencySpec,
      errorSpec,
      fromSparkSpec
    )
}
