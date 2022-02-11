package zio.spark.sql

import org.apache.spark.sql.AnalysisException

import scala.util.Try

/**
 * A Try like structure to describe a transformation that can fail with
 * an AnalysisException. Generally speaking when you make a
 * transformation on your dataset, for some transformations, Spark
 * throws an AnalysisException, these transformations are wrapped into a
 * TryAnalysis.
 *
 * You can ignore the TryAnalysis wrapper (and make Spark fails as usual
 * when a impossible transformation is being build (like selecting a
 * column that don't exist)) using:
 * {{{
 * scala> import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
 * }}}
 */
sealed trait TryAnalysis[+T] {
  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  @throws[AnalysisException]
  final def getOrThrow: T =
    this match {
      case TryAnalysis.Failure(e) => throw e
      case TryAnalysis.Success(v) => v
    }
}

object TryAnalysis {
  final case class Failure(analysisException: AnalysisException) extends TryAnalysis[Nothing]
  final case class Success[T](value: T)                          extends TryAnalysis[T]

  def apply[T](t: => T): TryAnalysis[T] =
    try Success(t)
    catch {
      case analysisException: AnalysisException => Failure(analysisException)
    }

  implicit final class Ops[+T](tryAnalysis: => TryAnalysis[T]) {
    // only eval once if needed
    private lazy val eval: TryAnalysis[T] = TryAnalysis(tryAnalysis.getOrThrow)

    /** Recovers from an Analysis Exception. */
    def recover[U >: T](failure: AnalysisException => U): U = fold(failure, identity)

    /** Folds a TryAnalysis into a type B. */
    def fold[B](failure: AnalysisException => B, success: T => B): B =
      eval match {
        case TryAnalysis.Failure(e) => failure(e)
        case TryAnalysis.Success(v) => success(v)
      }

    /** Converts a TryAnalysis into an Either. */
    def toEither: Either[AnalysisException, T] = fold(Left.apply, Right.apply)

    /** Converts a TryAnalysis into a Try. */
    def toTry: Try[T] = fold(scala.util.Failure.apply, scala.util.Success.apply)
  }

  object syntax {
    @SuppressWarnings(Array("scalafix:DisableSyntax.implicitConversion"))
    @throws[AnalysisException]
    implicit def throwAnalysisException[T](analysis: TryAnalysis[T]): T = analysis.getOrThrow
  }
}
