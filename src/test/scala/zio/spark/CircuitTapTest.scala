package zio.spark

import zio.test._
import zio.test.Assertion._

object CircuitTapTest extends DefaultRunnableSpec {

  import zio.test._

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] =
    suite("tap")(
      testM("smoking")({
        import syntax._

        for {
          percent <- Ratio(0.05).toTask
          tap     <- CircuitTap.make[String, String](percent, _ => true, "rejected", 1000)
          _       <- tap("first".fail).ignore
          _       <- tap("second".fail).ignore
          a       <- tap("third".fail).either
          s       <- tap.getState
        } yield assert(a.isLeft)(isTrue) &&
          assert(s.failed)(equalTo(1L)) &&
          assert(s.rejected)(equalTo(2L)) &&
          assert(s.decayingErrorRatio.ratio.value)(isGreaterThan(Ratio.zero.value))
      })
    )

}
