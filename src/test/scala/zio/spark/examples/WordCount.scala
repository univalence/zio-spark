package zio.spark.examples

import zio.spark.{ SIO, SparkEnv, ZDataFrame, ZDataset, ZRDD }
import zio._
import zio.console.Console

object Context {

  val ss: TaskLayer[SparkEnv] =
    zio.spark.builder.master("local").appName("wordCount").getOrCreate
}

import Context._
import zio.spark.implicits._
import zio.spark.syntax._

object WordCount extends zio.App {

  val extract: SIO[ZDataset[String]] = spark.read.textFile("build.sbt")

  def transform(textFile: ZDataset[String]): ZRDD[(String, Int)] =
    textFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .rdd
      .reduceByKey(_ + _)

  def load(counts: ZRDD[(String, Int)]): Task[Unit] =
    counts.saveAsTextFile("target/spark/wordcount.txt")

  val pipeline: SIO[Unit] = extract >>- transform >>= load

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] =
    pipeline.provideCustomLayer(ss).exitCode

}

object PIEstimation extends zio.App {
  /*
  val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
  val x = math.random
  val y = math.random
  x*x + y*y < 1
  }.count()
  println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
   */
  val NUM_SAMPLES = 100000
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val getRdd: RIO[SparkEnv, ZRDD[Int]] = zio.spark.retroCompat(ss => {
      val landInCircle = ss.sparkContext.parallelize(1 to NUM_SAMPLES).filter { _ =>
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }
      landInCircle
    })

    val prg: RIO[Console with SparkEnv, Unit] = for {
      rdd   <- getRdd
      count <- rdd.count
      _     <- zio.console.putStrLn(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
    } yield ()

    prg.provideCustomLayer(ss).exitCode
  }
}

object TextSearch extends zio.App {

  /*
  val textFile = sc.textFile("hdfs://...")

// Creates a DataFrame having a single column named "line"
val df = textFile.toDF("line")
val errors = df.filter(col("line").like("%ERROR%"))
// Counts all the error
errors.count()
    // Counts errors mentioning MySQL errors.filter(col("line").like("%MySQL%")).count()
    // Fetches the MySQL errors as an array of strings errors.filter(col("line").like("%MySQL%")).collect()

   */

  val getTextFile: RIO[SparkEnv, ZRDD[String]] = zio.spark.sparkContext.textFile("hdfs://...")

  import zio.spark.implicits._
  import org.apache.spark.sql.functions._

  def filterErrors(textFile: ZRDD[String]): RIO[SparkEnv, ZDataFrame] =
    textFile.toDF("line").filter(col("line").like("%ERROR%"))

  val prg: RIO[SparkEnv, (Long, Long)] = for {
    textFile   <- getTextFile
    errors     <- filterErrors(textFile)
    count      <- errors.count
    countMysql <- errors.filter(col("line").like("%MYSQL")).count
  } yield (count, countMysql)

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] = ???
}
