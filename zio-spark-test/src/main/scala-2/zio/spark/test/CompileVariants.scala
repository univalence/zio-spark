package zio.spark.test

import zio.spark.sql.SIO
import zio.test.TestResult

trait CompileVariants {
  def assertZIOSpark[A, B](value: SIO[A])(assertion: SparkAssertion[A, B]): SIO[TestResult] = macro Macros.assert_impl

  // TODO
  def assertSpark[A, B](value: A)(assertion: SparkAssertion[A, B]): SIO[TestResult] = ???
  // assertZIOSpark(ZIO.succeed(value))(assertion)
}
