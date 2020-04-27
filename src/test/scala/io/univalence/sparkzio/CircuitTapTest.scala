package io.univalence.sparkzio

import zio.test._
import zio.test.Assertion._

object CircuitTapTest
    extends DefaultRunnableSpec(
      suite("tap")(
        testM("smoking")({
          import zio.syntax._
          import syntax._

          for {
            percent <- Ratio(0.05).toTask
            tap     <- CircuitTap.make[String, String](percent, _ => true, "rejected", 1000)
            _       <- tap("first".fail).ignore
            _       <- tap("second".fail).ignore
            a       <- tap("third".fail).either
            s       <- tap.getState
          } yield {
            assert(a.isLeft, isTrue) &&
            assert(s.failed, equalTo(1)) &&
            assert(s.rejected, equalTo(2)) &&
            assert(s.decayingErrorRatio.ratio.value, isGreaterThan(Ratio.zero.value))
          }
        })
      )
    )
