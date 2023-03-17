package zio.spark.test.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

sealed trait ValueMatcher {
  def process[T](current: T, schema: StructType): Boolean
}

object ValueMatcher {
  sealed trait PositionalValueMatcher extends ValueMatcher {
    override def process[T](current: T, schema: StructType): Boolean =
      this match {
        case PositionalValueMatcher.Value(expected) =>
          expected match {
            case expected: Row =>
              current match {
                case current: Row => expected.toSeq.sameElements(current.toSeq)
                case _            => false
              }
            case expected: T => current == expected
            case _           => false
          }
        case PositionalValueMatcher.Anything => true
        case PositionalValueMatcher.Predicate(predicate) =>
          predicate match {
            case predicate: (T => Boolean) => predicate(current)
            case _                         => false
          }
        case PositionalValueMatcher.Or(left, right) =>
          left.process(current, schema) || right.process(current, schema)
        case PositionalValueMatcher.And(left, right) =>
          left.process(current, schema) && right.process(current, schema)
      }

    def &&(that: PositionalValueMatcher) = PositionalValueMatcher.And(this, that)

    def ||(that: PositionalValueMatcher) = PositionalValueMatcher.Or(this, that)
  }

  sealed trait GlobalValueMatcher extends ValueMatcher {
    override def process[T](current: T, schema: StructType): Boolean =
      this match {
        case GlobalValueMatcher.KeyValue(key, matcher) =>
          current match {
            case current: Row           => processRow(current, key, matcher, schema)
            case t: T if key == "value" => matcher.process(t, schema)
            case _                      => false
          }
      }
  }

  object PositionalValueMatcher {
    final case class Value[T](value: T) extends PositionalValueMatcher
    case object Anything                extends PositionalValueMatcher

    final case class Predicate[T](predicate: T => Boolean) extends PositionalValueMatcher

    final case class Or(left: PositionalValueMatcher, right: PositionalValueMatcher) extends PositionalValueMatcher

    final case class And(left: PositionalValueMatcher, right: PositionalValueMatcher) extends PositionalValueMatcher
  }

  object GlobalValueMatcher {
    final case class KeyValue(key: String, matcher: PositionalValueMatcher) extends GlobalValueMatcher
  }

  private def processRow(row: Row, key: String, matcher: PositionalValueMatcher, schema: StructType) =
    schema.zipWithIndex.filter(_._1.name == key).map(_._2).headOption match {
      case Some(pos) =>
        Option(row.get(pos)) match {
          case Some(curr) => matcher.process(curr, schema)
          case None       => false // The row is not large enough
        }
      case None => false // There is no field named $k
    }
}
