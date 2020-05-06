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

abstract class ZDataX[T](dataset: Dataset[T]) extends ZWrap(dataset) {
  final def write: Write = {
    class WriteImpl(task: Task[ZWrap[DataFrameWriter[T]]]) extends Write {
      override def option(key: String, value: String): Write = copy(_.option(key, value))

      def copy(f: DataFrameWriter[T] => DataFrameWriter[T]): Write = new WriteImpl(task.flatMap(_.execute(f)))

      override def format(name: String): Write    = copy(_.format(name))
      override def mode(writeMode: String): Write = copy(_.mode(writeMode))

      override def parquet(path: String): Task[Unit] = execute(_.parquet(path))

      override def text(path: String): Task[Unit] = execute(_.text(path))

      def execute(f: DataFrameWriter[T] => Unit): Task[Unit] = task >>= (_.execute(f))

      override def save(path: String): Task[Unit] = execute(_.save(path))
    }
    new WriteImpl(execute(_.write))
  }

  final def sparkSession: ZSparkSession = unsafeTotal(_.sparkSession)

  final def col(colName: String): Try[Column] = unsafe(_.col(colName))

  final def apply(colName: String): Try[Column] = unsafe(_(colName))

  final def cache: Task[Unit] = execute(_.cache()).unit

  final def createTempView(viewName: String): Task[Unit] = execute(_.createTempView(viewName))

  trait Write {
    def option(key: String, value: String): Write
    def format(name: String): Write
    def mode(writeMode: String): Write

    def parquet(path: String): Task[Unit]
    def text(path: String): Task[Unit]
    def save(path: String): Task[Unit]
  }

}

final class ZDataFrame(dataFrame: DataFrame) extends ZDataX(dataFrame) {
  def rdd: ZRDD[Row] = unsafeTotal(_.rdd)

  def collect(): Task[Seq[Row]] = execute(_.collect().toSeq)
}

final class ZDataset[T](dataset: Dataset[T]) extends ZDataX(dataset) {
  def map[B: Encoder](f: T => B): ZDataset[B] = unsafeTotal(_.map(f))

  def collect(): Task[Seq[T]] = executeNoWrap(_.collect().toSeq)

  def take(n: Int): Task[Seq[T]] = executeNoWrap(_.take(n).toSeq)

}
