package zio.test

import zio.test.Macros.showExpression_impl

import scala.reflect.macros._

// scalafix:off
private[zio] object MacrosZioSpark {
  def assert_impl(c: blackbox.Context)(value: c.Tree)(assertion: c.Tree): c.Tree = {
    import c.universe._

    val codeString      = showExpression_impl(c)(value)
    val assertionString = showExpression_impl(c)(assertion)
    q"_root_.zio.spark.test.assertSparkImpl($value, $codeString, $assertionString)($assertion)"
  }

  def assertZIO_impl(c: blackbox.Context)(value: c.Tree)(assertion: c.Tree): c.Tree = {
    import c.universe._

    val codeString      = showExpression_impl(c)(value)
    val assertionString = showExpression_impl(c)(assertion)

    q"_root_.zio.spark.test.assertZIOSparkImpl($value, $codeString, $assertionString)($assertion)"
  }
}
