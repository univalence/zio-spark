package zio.spark

import zio.console.Console
import zio.{ ZIO, ZLayer }

object Sample extends zio.App {

  import zio.spark.implicits._

  val prg: ZIO[Console with SparkEnv, Throwable, Int] = for {
    dataFrame <- zio.spark.read.textFile("src/test/resources/toto")
    dataset   <- dataFrame.as[String].toTask
    seq       <- dataset.take(1)
    _         <- zio.console.putStrLn(seq.head)
  } yield { 0 }

  val ss: ZLayer[Any, Throwable, SparkEnv] =
    zio.spark.builder.master("local").appName("xxx").getOrCreate

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    prg.provideCustomLayer(ss).orDie

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
