package zio.spark.effect

import zio.{Task, UIO}
import zio.spark.SparkSessionRunner.session
import zio.spark.rdd.RDD
import zio.spark.sql.Spark
import zio.test._

object MapWithEffectSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("smoke") {
      test("basic smoke test") {
        val getRddInt: Spark[RDD[Int]] =
          zio.spark.sql.fromSpark { ss =>
            import ss.implicits._
            RDD(Seq(1, 2, 3).toDS().rdd)
          }

        def effect(rdd: RDD[Int]): Task[Seq[Either[String, Unit]]] =
          MapWithEffect(rdd.map(i => UIO(println(i))))("rejected").collect

        (getRddInt flatMap effect).map(seq => assertTrue(seq.size == 3))
      }
    }.provideCustomLayerShared(session)
}
