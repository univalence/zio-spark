package io.univalence.sparkzio

import org.scalatest.FunSuite
import zio.console.Console
import zio.{ DefaultRuntime, IO, RIO, Task, ZIO }

class CircuitTapTest extends FunSuite {

  import zio.syntax._

  test("tap") {

    import syntax._

    val e3: IO[String, String] = "third".fail

    val prg: RIO[Console, Unit] = for {
      percent <- Ratio(0.05).toTask
      tap     <- CircuitTap.make[String, String](percent, _ => true, "rejected", 1000)
      _       <- tap("first".fail).ignore
      _       <- tap.getState.flatMap(s => ZIO.effect(println(s)))
      _       <- tap("second".fail).ignore
      a       <- tap(e3).either
      s       <- tap.getState
      _       <- zio.console.putStrLn((a, s).toString)
    } yield {
      assert(a.isLeft)
      assert(s.failed == 1)
      assert(s.rejected == 2)
      assert(s.decayingErrorRatio.ratio > Ratio.zero)
    }

    new DefaultRuntime {}.unsafeRunSync(prg)

  }
}
