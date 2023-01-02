package zio.spark.test

import zio.Trace
import zio.spark.sql.SIO
import zio.test.TestResult
import zio.internal.stacktracer.SourceLocation

trait CompileVariants {
  inline def assertZIOSpark[A, B](inline value: SIO[A])(inline assertion: SparkAssertion[A, B])(implicit trace: Trace, sourceLocation: SourceLocation): SIO[TestResult] =
    ${Macros.assert_impl('value)('assertion, 'trace, 'sourceLocation)}
}

object CompileVariants {
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