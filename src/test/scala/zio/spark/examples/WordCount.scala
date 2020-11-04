package zio.spark.examples

import zio._
import zio.console.Console
import zio.spark.{ Spark, SparkEnv, ZDataFrame, ZDataset, ZRDD, ZRelationalGroupedDataset }

import scala.util.Try

object Context {

  def localSparkSession(name: String): TaskLayer[SparkEnv] =
    zio.spark.builder.master("local").appName(name).getOrCreate
}

import zio.spark.examples.Context._
import zio.spark.implicits._
import zio.spark.syntax._

object WordCount extends zio.App {

  val read: Spark[ZDataset[String]] = spark.read.textFile("build.sbt")

  def transform(textFile: ZDataset[String]): ZRDD[(String, Int)] =
    textFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .rdd
      .reduceByKey(_ + _)

  def save(counts: ZRDD[(String, Int)]): Task[Unit] =
    counts.saveAsTextFile("target/output/wordcount.txt")

  val pipeline: Spark[Unit] = read >>- transform >>= save

  private val session = localSparkSession("wordCount")

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] =
    pipeline.provideCustomLayer(session).exitCode

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
    val getRdd: RIO[SparkEnv, ZRDD[Int]] = zio.spark.retroCompat { ss =>
      val landInCircle = ss.sparkContext.parallelize(1 to NUM_SAMPLES).filter { _ =>
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }
      landInCircle
    }

    val prg: RIO[Console with SparkEnv, Unit] = for {
      rdd   <- getRdd
      count <- rdd.count
      _     <- zio.console.putStrLn(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
    } yield ()

    prg.provideCustomLayer(localSparkSession("PiEstimation")).exitCode
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

  val readTextFile: RIO[SparkEnv, ZRDD[String]] = zio.spark.sparkContext.textFile("hdfs://...")

  import org.apache.spark.sql.functions._
  import zio.spark.implicits._

  def filterErrors(textFile: ZRDD[String]): RIO[SparkEnv, ZDataFrame] =
    textFile.toDF("line").filter(col("line").like("%ERROR%"))

  val prg: RIO[SparkEnv, (Long, Long)] = for {
    textFile   <- readTextFile
    errors     <- filterErrors(textFile)
    count      <- errors.count
    countMysql <- errors.filter(col("line").like("%MYSQL")).count
  } yield (count, countMysql)

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] = ???
}

object SimpleDataOp extends zio.App {
  /*
// Creates a DataFrame based on a table named "people"
// stored in a MySQL database.
val url =
  "jdbc:mysql://yourIP:yourPort/test?user=yourUsername;password=yourPassword"
val df = sqlContext
  .read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", "people")
  .load()

// Looks the schema of this DataFrame.
df.printSchema()

// Counts people by age
val countsByAge = df.groupBy("age").count()
countsByAge.show()

// Saves countsByAge to S3 in the JSON format.
countsByAge.write.format("json").save("s3a://...")

   */

  import org.apache.spark.sql.functions._

  val url =
    "jdbc:mysql://yourIP:yourPort/test?user=yourUsername;password=yourPassword"

  val df: RIO[SparkEnv, ZDataFrame] = zio.spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "people")
    .load

  def groupByAge(df: ZDataFrame): Try[ZRelationalGroupedDataset] =
    df.groupBy(col("age"))

  val prg: ZIO[SparkEnv, Throwable, Unit] = for {
    df    <- df
    -     <- df.printSchema
    count <- groupByAge(df).count
    _     <- count.show
    _     <- count.write.format("json").save("s3a://...")

  } yield ()

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] = ???
}
