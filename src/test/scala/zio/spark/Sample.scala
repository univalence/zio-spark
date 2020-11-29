package zio.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.QueryExecution
import zio.console.Console
import zio.spark.wrap.{ Clean, Impure }
import zio._

object SampleCSV extends zio.App {

  import zio.spark.implicits._

  val ss: ZLayer[Any, Throwable, SparkEnv] =
    zio.spark.builder.master("local").appName("xxx").getOrCreate

  val csv: RIO[SparkEnv, ZDataFrame] = zio.spark.retroCompat(_.read.csv("src/test/resources/toto"))

  val prg: ZIO[Console with SparkEnv, Throwable, Unit] = for {
    //csv  <- zio.spark.read.format("CSV").load("src/test/resources/toto")
    csv <- zio.spark.read.csv("src/test/resources/toto")
    l   <- csv.count
    _   <- zio.console.putStrLn(s"count = $l")
  } yield {}

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] =
    prg.provideCustomLayer(ss).exitCode
}

object SyntaxOver9000 {
  import zio.spark.syntax.over9000._

  val tooOldToCode: Spark[ZDataFrame] = (
    zio.spark
      sql "select * from person"
      filter "age > 25"
  )

  val checkCsv: Spark[Long] = zio.spark.read.option("xxx", "yyy").csv("zzz").count
}

object Sample extends zio.App {
  import zio.spark.implicits._

  private val prgSpark: ZIO[SparkEnv, Throwable, Long] =
    zio.spark.read.option("xxx", "yyy").csv("coucou") >>= (_.count)

  val prg: ZIO[Console with SparkEnv, Throwable, Int] = for {
    dataFrame <- zio.spark.read.textFile("src/test/resources/toto")
    dataset   <- dataFrame.as[String].toTask
    seq       <- dataset.take(1)
    _         <- zio.console.putStrLn(seq.head)
    _         <- dataset.filter(_.contains("bonjour")).write.text("yolo")
  } yield 0

  val ss: ZLayer[Any, Throwable, SparkEnv] =
    zio.spark.builder.master("local").appName("xxx").getOrCreate

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    prg.provideCustomLayer(ss).exitCode

}

object Backport extends zio.App {

  zio.spark.retroCompat { ss =>
    val df = ss.read.json("examples/src/main/resources/people.json")

    df.show()
    import ss.implicits._
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")

    val sqlDF = ss.sql("SELECT * FROM people")
    sqlDF.show()
    df.createGlobalTempView("people")

    ss.sql("SELECT * FROM global_temp.people").show()
    ss.newSession().sql("SELECT * FROM global_temp.people").show()
  }

  def prg(ss: org.apache.spark.sql.SparkSession): org.apache.spark.sql.Dataset[String] = ???

  val zPrg: ZIO[SparkEnv, Throwable, ZDataset[String]] = zio.spark.sparkSession >>= (_.execute(prg))

  val zPrg_ : RIO[SparkEnv, ZDataset[String]] = zio.spark.retroCompat(prg)

  val zPrg2: ZIO[Console with SparkEnv, Throwable, Unit] = for {
    ds <- zPrg
    xs <- ds.take(10)
    _  <- zio.console.putStrLn(xs.toString)
  } yield {}

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] =
    zPrg2.provideCustomLayer(Sample.ss).exitCode

}

object Sample2 extends zio.App {

  import zio.spark.implicits._
  import zio.spark.syntax._
  import zio.spark.syntax.over9000._

  val df: Spark[ZDataset[String]] = zio.spark.read
    .option("xxx", "yyy")
    .format("text")
    .load("build.sbt") >>= (_.as[String])

  val rdd: Spark[ZRDD[String]] = df.map(_.rdd)

  override def run(args: List[String]) = ???
}

object Sample5 {

  import zio.spark.implicits._

  val ds: ZDataset[String] = ???

  ds.map(_.length)

  private val v1: Task[Impure[QueryExecution]] = ds.execute(_.queryExecution)

  private val v2: Task[String] = ds.execute(_ => "abc")

  private val v3: Task[ZRDD[String]] = ds.execute(_.rdd)

  val v6: Task[ZRDD[Int]] = Task(Clean({
    val sc: SparkContext = ???

    sc.parallelize(0 to 1000)
  }))

  v6 flatMap (_.count)

  private val v4: ZIO[Any, Throwable, Int] = v3.map(_.id)

  v1 flatMap (_.execute(_.analyzed))
}
/*
object Sample4 extends zio.App {

  implicit class ZIOExtractOps[R, A](rio: RIO[R, A]) {
    def resurrect: RIO[R, A] =
      rio.sandbox.mapError(c => c.squash)
  }

  val prg: Task[Unit] = ZIO.fail(new Exception("ahoy")) //.unit.orDie

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    prg.resurrect
      .catchAll(e => zio.console.putStrLn("🎉" + e.toString))
      .as(0)
}
 */
