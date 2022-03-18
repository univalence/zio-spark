package example

import org.apache.spark.sql.Row

import zio._
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._

object SimpleApp extends ZIOAppDefault {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

  final case class Person(name: String, age: Int)

  val filePath: String = "examples/simple-app/src/main/resources/data.csv"

  def read: SIO[DataFrame] = SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv(filePath)

  def transform(inputDs: DataFrame): Dataset[Person] = inputDs.as[Person]

  def output(transformedDs: Dataset[Person]): Task[Option[Person]] = transformedDs.headOption

  val pipeline: Pipeline[Row, Person, Option[Person]] = Pipeline(read, transform, output)

  val job: ZIO[SparkSession with Console, Throwable, Unit] =
    for {
      maybePeople <- pipeline.run
      _ <-
        maybePeople match {
          case None    => Console.printLine("There is nobody :(.")
          case Some(p) => Console.printLine(s"The first person's name is ${p.name}.")
        }
    } yield ()

  private val session = SparkSession.builder.master(localAllNodes).appName("app").getOrCreateLayer

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] = job.provideCustomLayer(session)
}
