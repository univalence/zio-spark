package zio.spark.test

import scala.reflect.macros._

import zio._
import zio.test.TestResult
import zio.test.Macros.showExpr
import zio.internal.stacktracer.SourceLocation

import zio.spark.sql.SIO
import zio.spark.test.SparkAssertion

import scala.quoted._

private[test] object Macros {

  // Pilfered (with immense gratitude & minor modifications)
  // from https://github.com/zio/zio/blob/series/2.x/test/shared/src/main/scala-3/zio/test/Macros.scala
  def assert_impl[A, B](value: Expr[SIO[A]])(assertion: Expr[SparkAssertion[A, B]], trace: Expr[Trace], sourceLocation: Expr[SourceLocation])(using Quotes, Type[A], Type[B] ): Expr[SIO[TestResult]] = {
    import quotes.reflect._
    val code = showExpr(value)
    val assertionCode = showExpr(assertion)
    '{_root_.zio.spark.test.assertZIOSparkImpl($value, ${Expr(code)}, ${Expr(assertionCode)})($assertion)($trace, $sourceLocation)
    }
  }
}
