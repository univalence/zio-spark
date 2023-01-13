package zio.spark.test.internal

import zio.spark.test.internal.ValueMatcher.GlobalValueMatcher._
import zio.spark.test.internal.ValueMatcher.{GlobalValueMatcher, PositionalValueMatcher}
trait ToGlobalValueMatcher[T] {
  def apply(t: T): GlobalValueMatcher
}

object ToGlobalValueMatcher {
  implicit def kv[T: ToPositionalValueMatcher]: ToGlobalValueMatcher[(String, T)] =
    new ToGlobalValueMatcher[(String, T)] {
      override def apply(t: (String, T)): GlobalValueMatcher =
        KeyValue(t._1, implicitly[ToPositionalValueMatcher[T]].apply(t._2))
    }
}