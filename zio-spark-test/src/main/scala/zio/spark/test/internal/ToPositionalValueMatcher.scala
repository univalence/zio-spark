package zio.spark.test.internal


import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher._
import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
trait ToPositionalValueMatcher[T] {
  def apply(t: T): PositionalValueMatcher
}

object ToPositionalValueMatcher {
  implicit def array[T]: ToPositionalValueMatcher[Seq[T]] = Value.apply
  implicit def map[K, V]: ToPositionalValueMatcher[Map[K, V]] = Value.apply

  implicit val boolean: ToPositionalValueMatcher[Boolean] = Value.apply
  implicit val byte: ToPositionalValueMatcher[Byte] = Value.apply
  implicit val char: ToPositionalValueMatcher[Char] = Value.apply
  implicit val datetimeLocal: ToPositionalValueMatcher[LocalDateTime] = Value.apply
  implicit val datetimeOffset: ToPositionalValueMatcher[OffsetDateTime] = Value.apply
  implicit val date: ToPositionalValueMatcher[LocalDate] = Value.apply
  implicit val decimal: ToPositionalValueMatcher[BigDecimal] = Value.apply
  implicit val double: ToPositionalValueMatcher[Double] = Value.apply
  implicit val float: ToPositionalValueMatcher[Float] = Value.apply
  implicit val int: ToPositionalValueMatcher[Int] = Value.apply
  implicit val long: ToPositionalValueMatcher[Long] = Value.apply
  implicit val short: ToPositionalValueMatcher[Short] = Value.apply
  implicit val string: ToPositionalValueMatcher[String] = Value.apply

  implicit def predicate[T]: ToPositionalValueMatcher[T => Boolean] = Predicate.apply
}