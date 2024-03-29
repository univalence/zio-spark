package zio.spark.sql

import org.apache.spark.sql.{ColumnName, Encoders}

import zio._
import zio.spark.rdd._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object implicits extends LowPrioritySQLImplicits {
  val given = "" // A little trick to be compliant with scala3

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

  /** An encoder for nullable timestamp type. */
  implicit def javaTimestamp: Encoder[java.sql.Timestamp] = Encoders.TIMESTAMP

  /** An encoder for arrays of bytes. */
  implicit def binary: Encoder[Array[Byte]] = Encoders.BINARY

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
    def toDataset: SIO[Dataset[T]] =
      zio.spark.sql.fromSpark(ss => ss.implicits.localSeqToDatasetHolder(seq).toDS().zioSpark).orDie

    def toDS(implicit trace: Trace): SIO[Dataset[T]] = toDataset
  }

  implicit class seqRddHolderOps[T: ClassTag](seq: Seq[T]) {
    def toRDD(implicit trace: Trace): SIO[RDD[T]] = ZIO.serviceWith[SparkSession](_.sparkContext.makeRDD(seq))
  }

}

trait LowPrioritySQLImplicits {
  implicit final def newProductEncoder[T <: Product: TypeTag]: implicits.Encoder[T] = Encoders.product[T]
}
