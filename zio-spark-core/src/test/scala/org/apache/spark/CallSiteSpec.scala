package org.apache.spark

import org.apache.log4j._
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, SparkListenerEvent, SparkListenerJobEnd}
import zio._
import zio.test._
import zio.spark.sql._
import zio.spark.parameter._

object CallSiteSpec extends ZIOSpecDefault {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] =
    SparkSession.builder
      .master(localAllNodes)
      .appName("zio-spark")
      .asLayer
      .orDie

  case class Person(name: String)

  val people = Seq(Person("John"))

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Call site spec")(
      test("zio-spark should return the same stack trace as spark") {
        for {
          events <- Ref.make[Chunk[SparkListenerJobEnd]](Chunk.empty)
          runtime <- ZIO.runtime[SparkSession]
          sc <- ZIO.serviceWith[SparkSession](_.sparkContext)
          listener <-
            ZIO.succeed(new SparkFirehoseListener {
              override def onEvent(event: SparkListenerEvent): Unit =
                event match {
                  case event: SparkListenerJobEnd =>
                    Unsafe.unsafe { implicit u =>
                      runtime.unsafe.run(events.update(_ :+ event)).getOrThrowFiberFailure()
                    }
                  case _ => ()
                }
            })
          _ <- ZIO.attempt(sc.underlying.addSparkListener(listener))
          e <- sc.textFile("build.sbt").flatMap(_.map(s => ???).count).either
          _ <- Console.printLine(e.merge)
          _ <- ZIO.attempt(Thread.sleep(1000))
          _ <- Console.printLine("-" * 50)
          spark <- ZIO.attempt(sc.underlying.textFile("build.sbt").map(s => ???).count).either
          _ <- Console.printLine(spark.merge)
          _ <- ZIO.attempt(Thread.sleep(1000000))
          finalEvents <- events.get
          result = finalEvents.head.jobResult match {
            case JobSucceeded => "success"
            case JobFailed(error) => error.getMessage
          }
        } yield assertTrue(result == "success")
      }
    ).provideLayer(session)
}
