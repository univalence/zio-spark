package zio.spark.sql

import org.apache.spark.sql.{ColumnName, Encoder, Encoders}

import scala.reflect.runtime.universe.TypeTag

trait SqlImplicits extends LowPrioritySQLImplicits {
  implicit final class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
  }

  /** An encoder for nullable string type. */
  implicit def javaString: Encoder[java.lang.String] = Encoders.STRING

  /**
   * An encoder for nullable date type.
   *
   * @since 1.6.0
   */
  implicit def javaDate: Encoder[java.sql.Date] = Encoders.DATE

  /**
   * Creates an encoder that serializes instances of the
   * `java.time.LocalDate` class to the internal representation of
   * nullable Catalyst's DateType.
   */
  implicit def javaLocalDate: Encoder[java.time.LocalDate] = Encoders.LOCALDATE

  /** An encoder for nullable timestamp type. */
  implicit def javaTimestamp: Encoder[java.sql.Timestamp] = Encoders.TIMESTAMP

  /**
   * Creates an encoder that serializes instances of the
   * `java.time.Instant` class to the internal representation of
   * nullable Catalyst's TimestampType.
   */
  implicit def javaInstant: Encoder[java.time.Instant] = Encoders.INSTANT

  /** An encoder for arrays of bytes. */
  implicit def binary: Encoder[Array[Byte]] = Encoders.BINARY

  /**
   * Creates an encoder that serializes instances of the
   * `java.time.Duration` class to the internal representation of
   * nullable Catalyst's DayTimeIntervalType.
   */
  implicit def javaDuration: Encoder[java.time.Duration] = Encoders.DURATION

  /**
   * Creates an encoder that serializes instances of the
   * `java.time.Period` class to the internal representation of nullable
   * Catalyst's YearMonthIntervalType.
   */
  implicit def javaPeriod: Encoder[java.time.Period] = Encoders.PERIOD

  /** An encoder for Scala's primitive int type. */
  implicit def scalaInt: Encoder[Int] = Encoders.scalaInt

  /** An encoder for Scala's primitive long type. */
  implicit def scalaLong: Encoder[Long] = Encoders.scalaLong

  /** An encoder for Scala's primitive double type. */
  implicit def scalaDouble: Encoder[Double] = Encoders.scalaDouble

  /** An encoder for Scala's primitive float type. */
  implicit def scalaFloat: Encoder[Float] = Encoders.scalaFloat

  /** An encoder for Scala's primitive byte type. */
  implicit def scalaByte: Encoder[Byte] = Encoders.scalaByte

  /** An encoder for Scala's primitive short type. */
  implicit def scalaShort: Encoder[Short] = Encoders.scalaShort

  /** An encoder for Scala's primitive boolean type. */
  implicit def scalaBoolean: Encoder[Boolean] = Encoders.scalaBoolean

}

trait LowPrioritySQLImplicits {
  implicit final def newProductEncoder[T <: Product: TypeTag]: Encoder[T] = Encoders.product[T]
}
