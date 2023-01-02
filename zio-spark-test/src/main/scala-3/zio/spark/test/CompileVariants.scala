package zio.spark.test

import zio.Trace
import zio.spark.sql.SIO
import zio.test.TestResult
import zio.internal.stacktracer.SourceLocation

trait CompileVariants {
  inline def assertSpark[A, B](inline value: => A) (inline assertion: SparkAssertion[A, B])(implicit trace: Trace, sourceLocation: SourceLocation):SIO[TestResult] =
  ${Macros.assertZIO_impl('value)('assertion, 'trace, 'sourceLocation)}

  inline def assertZIOSpark[A, B](inline value: SIO[A])(inline assertion: SparkAssertion[A, B])(implicit trace: Trace, sourceLocation: SourceLocation): SIO[TestResult] =
    ${Macros.assertZIO_impl('value)('assertion, 'trace, 'sourceLocation)}
}

object CompileVariants {
  def assertSparkProxy[A, B](
   value: => A,
   codePart: String,
   assertionPart: String
 )(
   assertion: SparkAssertion[A, B]
 )(implicit
   trace: Trace,
   sourceLocation: SourceLocation
 ) = zio.spark.test.assertSparkImpl(value, codePart, assertionPart)(assertion)

  def assertZIOSparkProxy[A, B](
    value: SIO[A],
    codePart: String,
    assertionPart: String
  )(
    assertion: SparkAssertion[A, B]
  )(implicit
    trace: Trace,
    sourceLocation: SourceLocation
  ) = zio.spark.test.assertZIOSparkImpl(value, codePart, assertionPart)(assertion)
}