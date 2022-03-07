import zio._
import zio.spark.parameter._
import zio.spark.rdd._
import zio.spark.sql._
import zio.spark.sql.implicits._

object WordCount extends ZIOAppDefault {

  val filePath: String = "build.sbt"

  def read: Spark[Dataset[String]] = SparkSession.read.textFile(filePath)

  def transform(inputDs: Dataset[String]): RDD[(String, Int)] =
    inputDs
      .flatMap(line => line.trim.replaceAll(" +", " ").split(" "))
      .map(word => (word, 1))
      .rdd
      .reduceByKey(_ + _)

  def output(transformedDs: RDD[(String, Int)]): Task[Seq[(String, Int)]] = transformedDs.collect

  val job: ZIO[SparkSession with Console, Throwable, Unit] =
    for {
      words <- read.map(transform).flatMap(output)
      mostUsedWord = words.sortBy(_._2).reverse.headOption
      _ <-
        mostUsedWord match {
          case None    => Console.printLine("The file is empty :(.")
          case Some(w) => Console.printLine(s"The most used word is '${w._1}'.")
        }
    } yield ()

  private val session = SparkSession.builder.master(localAllNodes).appName("zio-spark").getOrCreateLayer

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] = job.provideCustomLayer(session)
}
