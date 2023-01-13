package zio.spark.test.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import zio.spark.test.internal.ValueMatcher.{GlobalValueMatcher, PositionalValueMatcher}

sealed trait RowMatcher {

  def isUnique: Boolean

  def allowMultipleMatches: RowMatcher

  def process[T](current: T, schema: StructType, indexMapping: Map[Int, Int]): Boolean
}

object RowMatcher {
  final case class PositionalRowMatcher(
      matchers: Seq[PositionalValueMatcher],
      isUnique: Boolean
  ) extends RowMatcher {
    override def allowMultipleMatches: PositionalRowMatcher = copy(isUnique = false)

    override def process[T](current: T, schema: StructType, indexMapping: Map[Int, Int]): Boolean =
      current match {
        case current: Row =>
          if (matchers.length == indexMapping.size)
            matchers.zipWithIndex.forall { case (matcher, index) =>
              matcher.process(current.get(indexMapping(index)), schema)
            }
          else false
        case _ =>
          if (matchers.length == 1) matchers.head.process(current, schema)
          else false
      }
  }

  final case class GlobalRowMatcher(
      matchers: Seq[GlobalValueMatcher],
      isUnique: Boolean
  ) extends RowMatcher {
    override def allowMultipleMatches: GlobalRowMatcher = copy(isUnique = false)

    override def process[T](current: T, schema: StructType, indexMapping: Map[Int, Int]): Boolean =
      matchers.exists(_.process(current, schema))
  }
}
