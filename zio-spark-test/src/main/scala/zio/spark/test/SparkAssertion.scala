package zio.spark.test

import zio.internal.stacktracer.SourceLocation
import zio.spark.sql.{Dataset, SIO}
import zio.test.{Assertion, TestArrow, TestResult}
import zio.internal.ansi.AnsiStringOps
final case class SparkAssertion[A, B](f: A => SIO[B], assertion: Assertion[B])

object SparkAssertion {
  private[test] def smartAssert[A](
    expr: => A,
    codePart: String,
    assertionPart: String
  )(
    assertion: Assertion[A]
  )(implicit sourceLocation: SourceLocation): TestResult = {
    lazy val value0 = expr
    val completeString = codePart.blue + " did not satisfy " + assertionPart.cyan

    TestResult(
      (TestArrow.succeed(value0).withCode(codePart) >>> assertion.arrow)
        .withLocation
        .withCompleteCode(completeString)
    )
  }

  def isEmpty[A]: SparkAssertion[Dataset[A], Boolean] =
    SparkAssertion(_.isEmpty, Assertion.isTrue.label("Dataset was not empty"))
}
