package zio.spark.effect

import zio._
import zio.test._

object CircuitTapTest extends DefaultRunnableSpec {

  import zio.test._

  override def spec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("tap")(
      test("smoking") {
        for {
          percent <- Task.fromTry(Ratio(0.05))
          tap <-
            CircuitTap
              .make[String, String](maxError = percent, qualified = _ => true, rejected = "rejected", decayScale = 1000)
          tapFailure = (error: String) => tap(ZIO.fail(error)).either
          e1 <- tapFailure("first")
          e2 <- tapFailure("second")
          e3 <- tapFailure("third")
          s  <- tap.getState
        } yield assertTrue(
          e1 == Left("first") &&
            e2 == Left("rejected") &&
            e3 == Left("rejected") &&
            s.failed == 1L && s.rejected == 2L && s.decayingErrorRatio.ratio.value > Ratio.zero.value
        )
      }
    )

}
