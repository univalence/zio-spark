package zio.spark.experimental

import org.apache.spark.SparkContextCompatibility.removeSparkListener
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}
import scala3encoders.given // scalafix:ok

import zio.{durationInt, durationLong, Chunk, Ref, UIO, Unsafe, ZIO}
import zio.spark.sql.{fromSpark, SIO, SparkSession}
import zio.spark.sql.implicits._
import zio.spark.test._
import zio.test._
import zio.test.TestAspect.{ignore, timeout, withLiveClock}

object CancellableEffectSpec extends ZIOSparkSpecDefault {
  val getJobGroup: SIO[String] = zio.spark.sql.fromSpark(_.sparkContext.getLocalProperty("spark.jobGroup.id"))

  def listenSparkEvents[R, E, A](zio: ZIO[R, E, A]): ZIO[R with SparkSession, E, (Seq[SparkListenerEvent], A)] =
    for {
      events  <- Ref.make[Chunk[SparkListenerEvent]](Chunk.empty)
      runtime <- ZIO.runtime[R with SparkSession]
      sc      <- fromSpark(_.sparkContext).orDie
      listener <-
        ZIO.succeed(new SparkFirehoseListener {
          override def onEvent(event: SparkListenerEvent): Unit =
            Unsafe.unsafe { implicit u =>
              runtime.unsafe.run(events.update(_ :+ event)).getOrThrowFiberFailure()
            }
        })
      _ <- ZIO.succeed(sc.addSparkListener(listener))
      x <- zio
      _ <-
        ZIO
          .succeed(removeSparkListener(sc, listener))
          .delay(1.seconds)
      allEvents <- events.getAndSet(Chunk.empty)
    } yield (allEvents, x)

  def waitBlocking(seconds: Long): UIO[Long] = ZIO.unit.delay(seconds.seconds).as(seconds)

  def exists[T](itr: Iterable[T])(pred: PartialFunction[T, Boolean]): Boolean =
    itr.exists(pred.applyOrElse(_, (_: T) => false))

  // This test seems to be flaky and make the whole specs failed.
  // See: https://github.com/univalence/zio-spark/issues/304.
  def spec =
    suite("Test cancellable spark jobs")(
      test("Cancellable jobs should have a specific group Id") {
        CancellableEffect.makeItCancellable(getJobGroup).map(x => assertTrue(x.startsWith("cancellable-group")))
      },
      test("Spark job should be cancelable") {
        val job: SIO[Long] =
          CancellableEffect
            .makeItCancellable(Seq(1, 2, 3).toRDD flatMap (_.map(_ => Thread.sleep(100000L)).count))
            .disconnect

        listenSparkEvents(waitBlocking(5).race(job)).map { case (events, n) =>
          assertTrue(
            n == 5L,
            exists(events) { case js: SparkListenerJobStart =>
              exists(events) { case je: SparkListenerJobEnd =>
                je.jobId == js.jobId && je.jobResult.toString.contains("cancelled job group")
              }
            }
          )
        }
      } @@ timeout(45.seconds) @@ withLiveClock @@ ignore
    )
}
