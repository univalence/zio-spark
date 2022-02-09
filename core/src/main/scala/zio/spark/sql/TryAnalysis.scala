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
 * You can ignore the TryAnalysis wrapper (and make Spark fails when a
 * bad transformation happens) using:
 * {{{
 * scala> import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
 * }}}
 */
sealed trait TryAnalysis[+T] {
  @throws[AnalysisException]
  final def getOrThrowAnalysisException: T = this.fold(x => throw x, identity)

  /** Folds a TryAnalysis into a type B. */
  def fold[B](failure: AnalysisException => B, success: T => B): B =
    this match {
      case TryAnalysis.Failure(e) => failure(e)
      case TryAnalysis.Success(v) => success(v)
    }

  /** Converts a TryAnalysis into an Either. */
  def toEither: Either[AnalysisException, T] = fold(Left.apply, Right.apply)

  /** Converts a TryAnalysis into a Try. */
  def toTry: Try[T] = toEither.toTry
}

object TryAnalysis {
  implicit final class Ops[+T](tryAnalysis: => TryAnalysis[T]) {
    private lazy val eval = TryAnalysis(tryAnalysis.getOrThrowAnalysisException)

    /** Recovers from an Analysis Exception. */
    def recover[U >: T](failure: AnalysisException => U): U = eval.fold(failure, identity)
  }

  case class Failure(analysisException: AnalysisException) extends TryAnalysis[Nothing]
  case class Success[T](value: T)                          extends TryAnalysis[T]

  def apply[T](t: => T): TryAnalysis[T] =
    try Success(t)
    catch {
      case analysisException: AnalysisException => Failure(analysisException)
    }

  object syntax {
    @throws[AnalysisException]
    implicit def throwAnalysisException[T](analysis: TryAnalysis[T]): T = analysis.getOrThrowAnalysisException
  }
}
