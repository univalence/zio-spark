package zio.spark.test.internal


import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher._
import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher
trait ToPositionalValueMatcher[T] {
  def apply(t: T): PositionalValueMatcher
}

object ToPositionalValueMatcher {
  implicit val int: ToPositionalValueMatcher[Int] = t => Value(t)
  implicit val string: ToPositionalValueMatcher[String] = t => Value(t)
  implicit def predicate[T]: ToPositionalValueMatcher[T => Boolean] = f => Predicate(f)
}