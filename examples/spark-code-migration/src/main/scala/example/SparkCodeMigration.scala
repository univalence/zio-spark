package example

import zio._
import zio.spark.parameter._
import zio.spark.sql._

object SparkCodeMigration extends ZIOAppDefault {
  val NUM_SAMPLES: Int = 100000

  val computePiJob: SIO[Long] =
    fromSpark { ss =>
      val landInCircle =
        ss.sparkContext.parallelize(1 to NUM_SAMPLES).filter { _ =>
          val x = math.random()
          val y = math.random()
          x * x + y * y < 1
        }
      landInCircle.count()
    }

  val job: RIO[Console with SparkSession, Unit] =
    for {
      count <- computePiJob
      _     <- Console.printLine(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
    } yield ()

  private val session = SparkSession.builder.master(localAllNodes).appName("app").asLayer

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] = job.provideCustomLayer(session)
}
