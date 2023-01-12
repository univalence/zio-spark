package zio.spark.test.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import zio.spark.test.internal.ValueMatcher.{GlobalValueMatcher, PositionalValueMatcher}

sealed trait RowMatcher {
  import RowMatcher._

  def isUnique: Boolean

  def allowMultipleMatches: RowMatcher

  def process[T](current: T, maybeSchema: Option[StructType], indexMapping: Map[Int, Int]): Boolean =
    this match {
      case PositionalRowMatcher(matchers, _) =>
        current match {
          case current: Row =>
            if (matchers.length == indexMapping.size)
              matchers.zipWithIndex.forall { case (matcher, index) =>
                matcher.process(current.get(indexMapping(index)), maybeSchema)
              }
            else false
          case _ =>
            if (matchers.length == 1) matchers.head.process(current, maybeSchema)
            else false
        }
      case GlobalRowMatcher(matchers, _) =>
        matchers.exists(_.process(current, maybeSchema))
    }
}

object RowMatcher {
  final case class PositionalRowMatcher(
      matchers: Seq[PositionalValueMatcher],
      isUnique: Boolean
  ) extends RowMatcher {
    override def allowMultipleMatches: RowMatcher = copy(isUnique = false)
  }

  final case class GlobalRowMatcher(
      matchers: Seq[GlobalValueMatcher],
      isUnique: Boolean
  ) extends RowMatcher {
    override def allowMultipleMatches: RowMatcher = copy(isUnique = false)
  }
}
