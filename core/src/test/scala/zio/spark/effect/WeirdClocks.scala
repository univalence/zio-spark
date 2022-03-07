package zio.spark.effect

import zio._
import zio.test._
import zio.test.TestAspect.{ignore, timeout}

object WeirdClocks extends DefaultRunnableSpec {
  def wait(seconds: Int): ZIO[Console, Nothing, Int] =
    UIO(Thread.sleep(seconds * 1000))
      .as(seconds)
      .onInterrupt(_ => zio.Console.printLine(s"stopped Thread.sleep($seconds seconds)").orDie)

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("clock")(
      test("raceTest") {
        wait(5).race(wait(15)) map (n => assertTrue(n == 5))
      } @@ timeout(10.seconds) @@ ignore
    )
}
