package zio.spark.test.internal


import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher._
import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
trait ToPositionalValueMatcher[T] {
  def apply(t: T): PositionalValueMatcher
}

object ToPositionalValueMatcher {
  private implicit def scala211workaround[T](f: T => PositionalValueMatcher) =
    new ToPositionalValueMatcher[T] {
      override def apply(t: T): PositionalValueMatcher = f(t)
    }

  implicit def array[T]: ToPositionalValueMatcher[Seq[T]] = (t: Seq[T]) => Value(t)
  implicit def map[K, V]: ToPositionalValueMatcher[Map[K, V]] = (t: Map[K, V]) => Value(t)

  implicit val boolean: ToPositionalValueMatcher[Boolean] = (t: Boolean) => Value(t)
  implicit val byte: ToPositionalValueMatcher[Byte] = (t: Byte) => Value(t)
  implicit val char: ToPositionalValueMatcher[Char] = (t: Char) => Value(t)
  implicit val datetimeLocal: ToPositionalValueMatcher[LocalDateTime] = (t: LocalDateTime) => Value(t)
  implicit val datetimeOffset: ToPositionalValueMatcher[OffsetDateTime] = (t: OffsetDateTime) => Value(t)
  implicit val date: ToPositionalValueMatcher[LocalDate] = (t: LocalDate) => Value(t)
  implicit val decimal: ToPositionalValueMatcher[BigDecimal] = (t: BigDecimal) => Value(t)
  implicit val double: ToPositionalValueMatcher[Double] = (t: Double) => Value(t)
  implicit val float: ToPositionalValueMatcher[Float] = (t: Float) => Value(t)
  implicit val int: ToPositionalValueMatcher[Int] = (t: Int) => Value(t)
  implicit val long: ToPositionalValueMatcher[Long] = (t: Long) => Value(t)
  implicit val short: ToPositionalValueMatcher[Short] = (t: Short) => Value(t)
  implicit val string: ToPositionalValueMatcher[String] = (t: String) => Value(t)

  implicit def predicate[T]: ToPositionalValueMatcher[T => Boolean] = (f: T => Boolean) =>  Predicate(f)
}