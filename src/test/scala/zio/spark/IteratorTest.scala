package zio.spark

import zio._
import zio.clock.Clock
import zio.stream.{ Stream, ZSink, ZStream }
import zio.test.DefaultRunnableSpec
import zio.test._
import zio.test.Assertion._

object StreamTest {

  def assertForAll[R, E, A](zstream: ZStream[R, E, A])(f: A => TestResult): ZIO[R, E, TestResult] =
    zstream.fold(assert(Unit)(Assertion.anything))((as, a) => as && f(a))

  def isSorted[A: Ordering]: Assertion[Iterable[A]] =
    Assertion.assertion("sorted")()(x => {
      val y = x.toList
      y.sorted == y
    })
}
