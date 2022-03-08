package zio.spark.effect

import org.apache.spark.SparkContextCompatibility.removeSparkListener
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}

import zio.{durationLong, Chunk, Clock, UIO, ZIO, ZRef}
import zio.spark.SparkSessionRunner
import zio.spark.sql.{fromSpark, Spark, SparkSession}
import zio.spark.sql.implicits.seqRddHolderOps
import zio.test.{assertTrue, DefaultRunnableSpec, TestEnvironment, ZSpec}
import zio.test.TestAspect.timeout

object CancellableEffectSpec extends DefaultRunnableSpec {
  val getJobGroup: Spark[String] = zio.spark.sql.fromSpark(_.sparkContext.getLocalProperty("spark.jobGroup.id"))

  def listenSparkEvents[R, E, A](zio: ZIO[R, E, A]): ZIO[R with SparkSession, E, (Seq[SparkListenerEvent], A)] =
    for {
      events  <- ZRef.make[Chunk[SparkListenerEvent]](Chunk.empty)
      runtime <- ZIO.runtime[R with SparkSession]
      sc      <- fromSpark(_.sparkContext).orDie
      listener <-
        UIO(new SparkFirehoseListener {
          override def onEvent(event: SparkListenerEvent): Unit = runtime.unsafeRun(events.update(_ :+ event))
        })
      _ <- UIO(sc.addSparkListener(listener))
      x <- zio
      _ <-
        UIO(removeSparkListener(sc, listener))
          .delay(1.seconds)
          .provideSomeLayer(Clock.live) // wait a bit the last events to be publish
      allEvents <- events.getAndSet(Chunk.empty)
    } yield (allEvents, x)

  def waitBlocking(seconds: Int): UIO[Int] = UIO.blocking(UIO(Thread.sleep(seconds * 1000L))).as(seconds)

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("cancellable")(
      test("jobGroup") {
        CancellableEffect.makeItCancellable(getJobGroup).map(x => assertTrue(x.startsWith("cancellable-group")))
      },
      test("smoke") {
        val job: Spark[Long] =
          CancellableEffect.makeItCancellable(Seq(1, 2, 3).toRDD flatMap (_.map(_ => Thread.sleep(100000L)).count))

        listenSparkEvents(waitBlocking(5).race(job)).map { case (events, n) =>
          assertTrue(
            n == 5,
            events.exists {
              case js: SparkListenerJobStart =>
                events.exists {
                  case je: SparkListenerJobEnd =>
                    je.jobId == js.jobId && je.jobResult.toString.contains("cancelled job group")
                  case _ => false
                }
              case _ => false
            }
          )
        }
      }
    ).provideCustomLayerShared(SparkSessionRunner.session) @@ timeout(10.seconds)
}
