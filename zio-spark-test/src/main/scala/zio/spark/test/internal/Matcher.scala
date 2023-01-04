package zio.spark.test.internal

import org.apache.spark.sql.Row

sealed trait Matcher[T]

object Matcher {

  case object Anything {
    override def equals(obj: Any): Boolean = true
  }

  sealed trait SchemaMatcher extends Matcher[Nothing]
  sealed trait LineMatcher[T] extends Matcher[T] {
    def respect(current: T): Boolean
  }

  object LineMatcher {
    final case class RowMatcher(expected: Row) extends LineMatcher[Row] {
      override def respect(current: Row): Boolean = expected.toSeq.sameElements(current.toSeq)
    }
    final case class DataMatcher[T](value: T) extends LineMatcher[T] {
      override def respect(current: T): Boolean = value == current
    }
    final case class ConditionalMatcher[T](predicate: T => Boolean) extends LineMatcher[T] {
      override def respect(current: T): Boolean = predicate(current)
    }
  }
}
