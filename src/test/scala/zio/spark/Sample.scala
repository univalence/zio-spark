package zio.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.QueryExecution
import zio.console.Console
import zio.spark.wrap.{ Wrap, ZWrap }
import zio.{ Task, ZIO, ZLayer }

object Sample extends zio.App {

  import zio.spark.implicits._

  val prg: ZIO[Console with SparkEnv, Throwable, Int] = for {
    dataFrame <- zio.spark.read.textFile("src/test/resources/toto")
    dataset   <- dataFrame.as[String].toTask
    seq       <- dataset.take(1)
    _         <- zio.console.putStrLn(seq.head)
    _         <- dataset.filter(_.contains("bonjour")).write.text("yolo")
  } yield { 0 }

  val ss: ZLayer[Any, Throwable, SparkEnv] =
    zio.spark.builder.master("local").appName("xxx").getOrCreate

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    prg.provideCustomLayer(ss).orDie

}

object Sample5 {

  import zio.spark.implicits._

  val ds: ZDataset[String] = ???

  ds.map(_.length)

  private val v1: Task[ZWrap[QueryExecution]] = ds.execute(_.queryExecution)

  private val v2: Task[String] = ds.execute(_ => "abc")

  private val v3: Task[ZRDD[String]] = ds.execute(_.rdd)

  val v6: Task[ZRDD[Int]] = Task(Wrap({
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
