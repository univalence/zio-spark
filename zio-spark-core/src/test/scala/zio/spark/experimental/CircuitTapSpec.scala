package zio.spark.experimental

import zio.ZIO
import zio.spark.experimental.NewType.Ratio._
import zio.spark.experimental.NewType.{Ratio, Weight}
import zio.test.ZIOSpecDefault

object CircuitTapSpec extends ZIOSpecDefault {

  import zio.test._

  override def spec: Spec[Any, Throwable] =
    suite("Test circuit tap")(
      test("Smoking test") {
        val percent = Ratio(0.05)

        for {
          tap <-
            CircuitTap
              .make[String, String](
                maxError   = percent,
                qualified  = _ => true,
                rejected   = "rejected",
                decayScale = Weight(1000)
              )
          tapFailure = (error: String) => tap(ZIO.fail(error)).either
          e1 <- tapFailure("first")
          e2 <- tapFailure("second")
          e3 <- tapFailure("third")
          s  <- tap.getState
        } yield assertTrue(
          e1 == Left("first") &&
            e2 == Left("rejected") &&
            e3 == Left("rejected") &&
            s.failed == 1L && s.rejected == 2L && s.decayingErrorRatio.ratio > Ratio.zero
        )
      }
    )

}
