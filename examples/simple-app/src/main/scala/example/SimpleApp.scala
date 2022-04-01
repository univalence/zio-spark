package example

import zio._
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._

object SimpleApp extends ZIOAppDefault {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

  final case class Person(name: String, age: Int)

  val filePath: String = "examples/simple-app/src/main/resources/data.csv"

  def read: ZIO[SparkSession with Console with System, Throwable, DataFrame] =
    SparkSession.read.inferSchema.withHeader.withDelimiter(";").csvTest(filePath)

  def transform(inputDs: DataFrame): Dataset[Person] = inputDs.as[Person]

  def output(transformedDs: Dataset[Person]): ZIO[System with Console, Throwable, Option[Person]] =
    transformedDs.headOptionTest

  val job: ZIO[SparkSession with Console with Clock with System, Throwable, Unit] =
    for {
      maybePeople <- read.map(transform).flatMap(output)
      _           <- Clock.sleep(1000.seconds)
      _ <-
        maybePeople match {
          case None    => Console.printLine("There is nobody :(.")
          case Some(p) => Console.printLine(s"The first person's name is ${p.name}.")
        }
    } yield ()

  private val session = SparkSession.builder.master(localAllNodes).appName("app").asLayer

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] = job.provideCustomLayer(session)
}
