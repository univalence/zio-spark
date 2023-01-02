package zio.spark.test

import zio.spark.sql.SIO
import zio.test.TestResult

trait CompileVariants {
  def assertSpark[A, B](value: => A)(assertion: SparkAssertion[A, B]): SIO[TestResult] = macro Macros.assert_impl

  def assertZIOSpark[A, B](value: SIO[A])(assertion: SparkAssertion[A, B]): SIO[TestResult] =
    macro Macros.assertZIO_impl
}
