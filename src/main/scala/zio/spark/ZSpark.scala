package zio.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.sql._
import zio.spark.wrap.Wrap.NoWrap
import zio.spark.wrap.{ Wrap, ZWrap, ZWrapFImpure }
import zio.{ RIO, Task, UIO, ZIO }

import scala.reflect.ClassTag
import scala.util.Try

final class ZRDD[T](rdd: RDD[T]) extends ZWrap(rdd) {
  def toDF(colNames: String*)(implicit encoder: Encoder[T]): RIO[SparkEnv, ZDataFrame] =
    sparkSession.flatMap(zss => zss.execute(ss => ss.implicits.rddToDatasetHolder(rdd).toDF(colNames: _*)))

  def count: Task[Long] = execute(_.count())

  def name: UIO[String] = executeTotal(_.name)
  def id: Int           = nowTotal(_.id)

  def ++(rdd: RDD[T]): ZRDD[T]   = nowTotal(_.++(rdd))
  def ++(zrdd: ZRDD[T]): ZRDD[T] = nowTotal(x => zrdd.nowTotal(x ++ _))

  def mapPartitions[B: ClassTag](f: Iterator[T] => Iterator[B]): ZRDD[B] = nowTotal(_.mapPartitions(f))

  def map[B: ClassTag](f: T => B): ZRDD[B]          = nowTotal(_.map(f))
  def flatMap[B: ClassTag](f: T => Seq[B]): ZRDD[B] = nowTotal(_.flatMap(f))

  def collect: Task[Seq[T]] = execute(_.collect.toSeq)(Wrap.noWrap)

  def saveAsTextFile(path: String): Task[Unit] = execute(_.saveAsTextFile(path))
}

object ZRDD {
  implicit class ZPairRDD[K: ClassTag, V: ClassTag](zrdd: ZRDD[(K, V)]) {
    def reduceByKey(func: (V, V) => V): ZRDD[(K, V)] = zrdd.nowTotal(new PairRDDFunctions(_).reduceByKey(func))
  }
}

final class ZSparkContext(sparkContext: SparkContext) extends ZWrap(sparkContext) {
  def textFile(path: String): Task[ZRDD[String]] = execute(_.textFile(path))

  def parallelize[T: ClassTag](seq: Seq[T]): ZRDD[T] = nowTotal(_.parallelize(seq))

}

final class ZSparkSession(sparkSession: SparkSession) extends ZWrap(sparkSession) {

  def sparkContext: ZSparkContext = nowTotal(_.sparkContext)

  def sql(sqlText: String): Task[ZDataFrame] = execute(_.sql(sqlText))

  class Read(task: Task[ZWrap[DataFrameReader]]) extends ZWrapFImpure(task) {
    private val chain = makeChain(new Read(_))

    def option(key: String, value: String): Read = chain(_.option(key, value))
    def option(key: String, value: Long): Read   = chain(_.option(key, value))

    def load: Task[ZDataFrame]                         = execute(_.load())
    def parquet(path: String): Task[ZDataFrame]        = execute(_.parquet(path))
    def textFile(path: String): Task[ZDataset[String]] = execute(_.textFile(path))
  }

  def read: Read = new Read(execute(_.read))

}

abstract class ZDataX[T](dataset: Dataset[T]) extends ZWrap(dataset) {

  class Write(task: Task[ZWrap[DataFrameWriter[T]]]) extends ZWrapFImpure(task) {
    private val chain = makeChain(new Write(_))

    final def option(key: String, value: String): Write = chain(_.option(key, value))
    final def format(name: String): Write               = chain(_.format(name))
    final def mode(saveMode: String): Write             = chain(_.mode(saveMode = saveMode))

    final def parquet(path: String): Task[Unit] = execute(_.parquet(path))
    final def text(path: String): Task[Unit]    = execute(_.text(path))
    final def save(path: String): Task[Unit]    = execute(_.save(path))
  }

  final def write: Write = new Write(execute(_.write))

  final def as[X: Encoder]: Try[ZDataset[X]] = now(_.as[X])

  final def sparkSession: ZSparkSession = nowTotal(_.sparkSession)

  final def col(colName: String): Try[Column] = now(_.col(colName))

  final def apply(colName: String): Try[Column] = now(_.apply(colName))

  final def cache: Task[Unit] = execute(_.cache()).unit

  final def createTempView(viewName: String): Task[Unit] = execute(_.createTempView(viewName))

  final def rdd: ZRDD[T] = nowTotal(_.rdd)

  final def collect(): Task[Seq[T]] = execute(_.collect().toSeq)(Wrap.noWrap)

  def take(n: Int): Task[Seq[T]] = execute(_.take(n).toSeq)(Wrap.noWrap)
}

final class ZDataFrame(dataFrame: DataFrame) extends ZDataX(dataFrame) {
  def count: Task[Long]                          = execute(_.count())
  def filter(condition: Column): Try[ZDataFrame] = now(_.filter(condition))

}

final class ZDataset[T](dataset: Dataset[T]) extends ZDataX(dataset) {
  def filter(func: T => Boolean): ZDataset[T]          = nowTotal(_.filter(func))
  def map[B: Encoder](f: T => B): ZDataset[B]          = nowTotal(_.map(f))
  def flatMap[B: Encoder](f: T => Seq[B]): ZDataset[B] = nowTotal(_.flatMap(f))

}
