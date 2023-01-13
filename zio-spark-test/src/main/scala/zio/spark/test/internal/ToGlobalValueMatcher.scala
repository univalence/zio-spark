package zio.spark.test.internal

import zio.spark.test.internal.ValueMatcher.GlobalValueMatcher._
import zio.spark.test.internal.ValueMatcher.{GlobalValueMatcher, PositionalValueMatcher}
trait ToGlobalValueMatcher[T] {
  def apply(t: T): GlobalValueMatcher
}

object ToGlobalValueMatcher {
  implicit def kv[T: ToPositionalValueMatcher]: ToGlobalValueMatcher[(String, T)] =
    kv => KeyValue(kv._1, implicitly[ToPositionalValueMatcher[T]].apply(kv._2))
}