package zio.spark.test

import scala.reflect.macros._

import zio._
import zio.test.TestResult
import zio.test.Macros.showExpr
import zio.internal.stacktracer.SourceLocation

import zio.spark.sql.SIO
import zio.spark.test.SparkAssertion

import scala.quoted._

// Pilfered (with immense gratitude & minor modifications)
// from https://github.com/zio/zio/blob/series/2.x/test/shared/src/main/scala-3/zio/test/Macros.scala
object Macros {
  def assert_impl[A: Type, B: Type](value: Expr[A])(assertion: Expr[SparkAssertion[A, B]], trace: Expr[Trace], sourceLocation: Expr[SourceLocation])(using Quotes): Expr[SIO[TestResult]] = {
    import quotes.reflect._
    val codeString = showExpr(value)
    val assertionString = showExpr(assertion)
    '{_root_.zio.spark.test.CompileVariants.assertSparkProxy($value, ${Expr(codeString)}, ${Expr(assertionString)})($assertion)($trace, $sourceLocation)
    }
  }

  def assertZIO_impl[A: Type, B: Type](value: Expr[SIO[A]])(assertion: Expr[SparkAssertion[A, B]], trace: Expr[Trace], sourceLocation: Expr[SourceLocation])(using Quotes): Expr[SIO[TestResult]] = {
    import quotes.reflect._
    val codeString = showExpr(value)
    val assertionString = showExpr(assertion)
    '{_root_.zio.spark.test.CompileVariants.assertZIOSparkProxy($value, ${Expr(codeString)}, ${Expr(assertionString)})($assertion)($trace, $sourceLocation)
    }
  }

  def showExpr[A](expr: Expr[A])(using Quotes): String = {
    import quotes.reflect._
    expr.asTerm.pos.sourceCode.get
  }
}
