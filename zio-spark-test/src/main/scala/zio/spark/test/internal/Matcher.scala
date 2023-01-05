package zio.spark.test.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import zio.spark.test.internal.ValueMatcher.{GlobalValueMatcher, PositionalValueMatcher}

sealed trait Matcher

object Matcher {
  case object SchemaMatcher extends Matcher
  sealed trait RowMatcher extends Matcher

  object RowMatcher {
    case class PositionalRowMatcher(matchers: Seq[PositionalValueMatcher]) extends RowMatcher
    case class GlobalRowMatcher(matchers: Seq[GlobalValueMatcher]) extends RowMatcher

    def process[T](matcher: RowMatcher, current: T, maybeSchema: Option[StructType]) = matcher match {
      case PositionalRowMatcher(matchers) =>
        current match {
          case current: Row => ???
        }
      case GlobalRowMatcher(matchers) =>
        matchers.exists(matcher => ValueMatcher.process(current, maybeSchema, matcher))
    }
  }
}
