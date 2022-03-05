package zio.spark.effect

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

import zio.{durationLong, UIO, ZIO}
import zio.spark.SparkSessionRunner
import zio.spark.sql.Spark
import zio.spark.sql.implicits.seqRddHolderOps
import zio.test.{assertTrue, DefaultRunnableSpec, TestClock, TestEnvironment, ZSpec}
import zio.test.TestAspect.{ignore, timeout}

object CancellableEffectSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("cancellable")(
      test("smoke") {
        val job: Spark[Long] =
          CancellableEffect.makeItCancellable(Seq(1, 2, 3).toRDD flatMap (_.map(_ => Thread.sleep(10000)).count))

        val small: ZIO[Any, Nothing, Long] = UIO(Thread.sleep(5000)).as(5L)

        val addListennerToSpark =
          zio.spark.sql.fromSpark(_.sparkContext.addSparkListener(new SparkListener {
            override def onJobStart(jobStart: SparkListenerJobStart): Unit = println(jobStart)

            override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = println(jobEnd)
          }))

        TestClock.adjust(10.seconds) *> addListennerToSpark *> small.race(job).map(x => assertTrue(x == 5))
      } @@ ignore
    ).provideCustomLayerShared(SparkSessionRunner.session) @@ timeout(5.seconds)
}
