package zio.spark.effect

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.{IO, Task, UIO}
import zio.spark.ZioSparkTestSpec.session
import zio.spark.effect.MapWithEffect._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.test._

object MapWithEffectSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("smoke")(
      test("basic smoke test") {
        val getRddInt: SIO[RDD[Int]] =
          zio.spark.sql.fromSpark { ss =>
            import ss.implicits._
            val ds: UnderlyingDataset[Int] = Seq(1, 2, 3).toDS()
            ds.zioSpark.rdd
          }

        Seq(1, 2, 3).toDataset.map(_.rdd)

        def effect(rdd: RDD[Int]): Task[Seq[Either[String, Int]]] =
          MapWithEffect(rdd.map(i => UIO(i)))("rejected").collect

        (getRddInt flatMap effect).map(seq => assertTrue(seq.size == 3))
      },
      test("failure") {
        Seq
          .fill(10000)(1)
          .toRDD
          .map(_.mapZIO(_ => IO.fail("fail").as(1), _ => "rejected"))
          .flatMap(_.collect)
          .map { res =>
            val size  = res.size
            val count = res.count(_ == Left("rejected"))
            val i     = res.indexWhere(_ == Left("rejected"))
            assertTrue(size == 10000, i == 1, count.toDouble < (0.95d * size))
          }
      }
    ).provideCustomLayerShared(session)
}
