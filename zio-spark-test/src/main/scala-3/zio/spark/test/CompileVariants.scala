package zio.spark.test

import zio.Trace
import zio.spark.sql.SIO
import zio.test.TestResult
import zio.internal.stacktracer.SourceLocation

trait CompileVariants {
  inline def assertZIOSpark[A, B](value: SIO[A])(assertion: SparkAssertion[A, B])(implicit trace: Trace, sourceLocation: SourceLocation): SIO[TestResult] =
    ${Macros.assert_impl('value)('assertion, 'trace, 'sourceLocation)}

  // TODO
  def assertSpark[A, B](value: A)(assertion: SparkAssertion[A, B]): SIO[TestResult] = ???
  // assertZIOSpark(ZIO.succeed(value))(assertion)
}
