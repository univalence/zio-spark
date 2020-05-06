package zio.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoder, Row, SparkSession }
import zio.{ Task, UIO }

import scala.reflect.ClassTag
import scala.util.Try

final class ZRDD[T](rdd: RDD[T]) extends ZWrap(rdd) {
  def name: UIO[String] = executeTotal(_.name)
  def id: Int           = unsafeTotal(_.id)

  def ++(rdd: RDD[T]): ZRDD[T]   = unsafeTotal(_.++(rdd))
  def ++(zrdd: ZRDD[T]): ZRDD[T] = unsafeTotal(x => zrdd.unsafeTotal(x ++ _))

  def map[B: ClassTag](f: T => B): ZRDD[B]          = unsafeTotal(_.map(f))
  def flatMap[B: ClassTag](f: T => Seq[B]): ZRDD[B] = unsafeTotal(_.flatMap(f))
}

final class ZSparkSession(sparkSession: SparkSession) extends ZWrap(sparkSession) {
  def sql(sqlText: String): Task[ZDataFrame] = execute(_.sql(sqlText))
}

final class ZDataFrame(dataFrame: DataFrame) extends ZWrap(dataFrame) {
  def col(colName: String): Try[Column]   = unsafe(_.col(colName))
  def sparkSession: ZSparkSession         = unsafeTotal(_.sparkSession)
  def rdd: ZRDD[Row]                      = unsafeTotal(_.rdd)
  def collect(): Task[Seq[Row]]           = execute(_.collect().toSeq)
  def apply(colName: String): Try[Column] = unsafe(_(colName))
}

final class ZDataset[T](dataset: Dataset[T]) extends ZWrap(dataset) {
  def map[B: Encoder](f: T => B): ZDataset[B] = unsafeTotal(_.map(f))

}
