package zio.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import zio.{ Task, UIO }

import scala.reflect.ClassTag
import scala.util.Try

final class ZRDD[T](rdd: RDD[T]) extends ZWrap(rdd) {
  def count: Task[Long] = execute(_.count())

  def name: UIO[String] = executeTotal(_.name)
  def id: Int           = unsafeTotal(_.id)

  def ++(rdd: RDD[T]): ZRDD[T]   = unsafeTotal(_.++(rdd))
  def ++(zrdd: ZRDD[T]): ZRDD[T] = unsafeTotal(x => zrdd.unsafeTotal(x ++ _))

  def mapPartitions[B: ClassTag](f: Iterator[T] => Iterator[B]): ZRDD[B] = unsafeTotal(_.mapPartitions(f))

  def map[B: ClassTag](f: T => B): ZRDD[B]          = unsafeTotal(_.map(f))
  def flatMap[B: ClassTag](f: T => Seq[B]): ZRDD[B] = unsafeTotal(_.flatMap(f))
}

final class ZSparkContext(sparkContext: SparkContext) extends ZWrap(sparkContext) {
  def parallelize[T: ClassTag](seq: Seq[T]): ZRDD[T] = unsafeTotal(_.parallelize(seq))

}

final class ZSparkSession(sparkSession: SparkSession) extends ZWrap(sparkSession) {

  def sparkContext: ZSparkContext = unsafeTotal(_.sparkContext)

  def sql(sqlText: String): Task[ZDataFrame] = execute(_.sql(sqlText))

  def read: Read = {
    //
    case class ReadImpl(config: Seq[(String, String)]) extends Read {
      override def option(key: String, value: String): Read = copy(config :+ ((key, value)))

      override def parquet(path: String): Task[ZDataFrame] = reader >>= (_.execute(_.parquet(path)))

      def reader: UIO[ZWrap[DataFrameReader]] =
        executeTotal(ss => {
          val read = ss.read

          config.foreach({
            case (k, v) => read.option(k, v)
          })

          read
        })

      override def textFile(path: String): Task[ZDataset[String]] = reader >>= (_.execute(_.textFile(path)))
    }

    ReadImpl(Vector.empty)

  }

  trait Read {
    def option(key: String, value: String): Read

    def parquet(path: String): Task[ZDataFrame]

    def textFile(path: String): Task[ZDataset[String]]
  }
}

final class ZDataFrame(dataFrame: DataFrame) extends ZWrap(dataFrame) {
  def col(colName: String): Try[Column] = unsafe(_.col(colName))

  def sparkSession: ZSparkSession = unsafeTotal(_.sparkSession)

  def rdd: ZRDD[Row] = unsafeTotal(_.rdd)

  def collect(): Task[Seq[Row]] = execute(_.collect().toSeq)

  def apply(colName: String): Try[Column] = unsafe(_(colName))

  def createTempView(viewName: String): Task[Unit] = execute(_.createTempView(viewName))
}

final class ZDataset[T](dataset: Dataset[T]) extends ZWrap(dataset) {

  def map[B: Encoder](f: T => B): ZDataset[B] = unsafeTotal(_.map(f))

  def collect(): Task[Seq[T]] = executeNoWrap(_.collect().toSeq)

  def take(n: Int): Task[Seq[T]] = executeNoWrap(_.take(n).toSeq)

}
