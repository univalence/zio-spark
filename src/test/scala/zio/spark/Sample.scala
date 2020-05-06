package zio.spark

import org.apache.spark.sql.SparkSession
import zio.{ RIO, Task, ZIO }

object Sample {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("xxx").getOrCreate()

    import ss.implicits._
    println(ss.read.textFile("src/test/resources/toto/").as[String].take(1)(0))

  }

}

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
