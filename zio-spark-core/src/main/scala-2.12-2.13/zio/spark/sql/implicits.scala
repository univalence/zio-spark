package zio.spark.sql

import org.apache.spark.sql.{ColumnName, Encoders}

import zio.{Trace, URIO}
import zio.spark.rdd.{RDD, RDDConversionOps}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object implicits extends LowPrioritySQLImplicits {
  implicit final class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
  }

  // avoid ambiguous implicits errors
  sealed trait ImplicitPriority
  type Encoder[T] = org.apache.spark.sql.Encoder[T] with ImplicitPriority
  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf", "scalafix:DisableSyntax.implicitConversion"))
  @inline implicit def implicitPriority[T](enc: org.apache.spark.sql.Encoder[T]): Encoder[T] =
    enc.asInstanceOf[Encoder[T]]

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

  implicit class seqDatasetHolderOps[T: org.apache.spark.sql.Encoder](seq: Seq[T]) {
    def toDataset(implicit trace: Trace): URIO[SparkSession, Dataset[T]] =
      zio.spark.sql.fromSpark(ss => ss.implicits.localSeqToDatasetHolder(seq).toDS().zioSpark).orDie

    def toDS(implicit trace: Trace): URIO[SparkSession, Dataset[T]] = toDataset
  }

  implicit class seqRddHolderOps[T: ClassTag](seq: Seq[T]) {
    def toRDD(implicit trace: Trace): URIO[SparkSession, RDD[T]] =
      zio.spark.sql.fromSpark(_.sparkContext.makeRDD(seq).zioSpark).orDie
  }

}

trait LowPrioritySQLImplicits {
  implicit final def newProductEncoder[T <: Product: TypeTag]: implicits.Encoder[T] = Encoders.product[T]
}
