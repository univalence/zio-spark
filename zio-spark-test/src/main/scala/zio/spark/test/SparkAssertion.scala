package zio.spark.test

import zio.internal.ansi.AnsiStringOps
import zio.internal.stacktracer.SourceLocation
import zio.spark.sql.{Dataset, SIO}
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
import zio.test.{Assertion, TestArrow, TestResult}

/**
 * Todo: instruction is just the content of f as a string such as
 * {{{_.isEmpty}}} becomes {{{"isEmpty"}}}. We should use a macro
 * instead.
 */
final case class SparkAssertion[A, B](f: A => SIO[B], assertion: Assertion[B], instruction: String)

object SparkAssertion {
  private[test] def smartAssert[A](
      expr: => A,
      codePart: String,
      assertionPart: String,
      instructionPart: String
  )(
      assertion: Assertion[A]
  )(implicit sourceLocation: SourceLocation): TestResult = {
    lazy val value0       = expr
    val completeString    = codePart.blue + " did not satisfy " + assertionPart.cyan
    val instructionString = s"$codePart.$instructionPart"

    TestResult(
      (TestArrow.succeed(value0).withCode(instructionString) >>> assertion.arrow).withLocation
        .withCompleteCode(completeString)
    )
  }

  def isEmpty[A]: SparkAssertion[Dataset[A], Boolean] = SparkAssertion(_.isEmpty, Assertion.isTrue, "isEmpty")
  def shouldExist[A](expr: String): SparkAssertion[Dataset[A], Boolean] =
    SparkAssertion(_.filter(expr).isEmpty, Assertion.isFalse, s"""filter("$expr").isEmpty""")

  def shouldNotExist[A](expr: String): SparkAssertion[Dataset[A], Boolean] =
    shouldExist[A](expr).copy(assertion = Assertion.isTrue)
}
