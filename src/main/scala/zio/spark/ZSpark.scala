package zio.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.sql._
import zio.spark.wrap.Clean.Pure
import zio.spark.wrap.{ Clean, Impure, ImpureF }
import zio.{ RIO, Task, UIO, ZIO }

import scala.reflect.ClassTag
import scala.util.Try

final class ZRDD[T](rdd: RDD[T]) extends Impure(rdd) {
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

  def collect: Task[Seq[T]] = execute(_.collect.toSeq)(Clean.pure)

  def saveAsTextFile(path: String): Task[Unit] = execute(_.saveAsTextFile(path))
}

object ZRDD {
  implicit class ZPairRDD[K: ClassTag, V: ClassTag](zrdd: ZRDD[(K, V)]) {
    def reduceByKey(func: (V, V) => V): ZRDD[(K, V)] = zrdd.nowTotal(new PairRDDFunctions(_).reduceByKey(func))
  }
}

final class ZSparkContext(sparkContext: SparkContext) extends Impure(sparkContext) {
  def textFile(path: String): Task[ZRDD[String]] = execute(_.textFile(path))

  def parallelize[T: ClassTag](seq: Seq[T]): ZRDD[T] = nowTotal(_.parallelize(seq))

}

final class ZSparkSession(sparkSession: SparkSession) extends Impure(sparkSession) {

  def sparkContext: ZSparkContext = nowTotal(_.sparkContext)

  def sql(sqlText: String): Task[ZDataFrame] = execute(_.sql(sqlText))

  implicit final class ZReader(task: Task[Impure[DataFrameReader]]) extends ImpureF(task) {
    def option(key: String, value: String): ZReader = execute(_.option(key, value))
    def option(key: String, value: Long): ZReader   = execute(_.option(key, value))

    def load: Task[ZDataFrame]                         = execute(_.load())
    def parquet(path: String): Task[ZDataFrame]        = execute(_.parquet(path))
    def textFile(path: String): Task[ZDataset[String]] = execute(_.textFile(path))
  }

  def read: ZReader = new ZReader(execute(_.read))
}

abstract class ZDataX[T](dataset: Dataset[T]) extends Impure(dataset) {

  final class ZWrite(task: Task[Impure[DataFrameWriter[T]]]) extends ImpureF(task) {
    private type V = DataFrameWriter[T]
    private def chain(f: V => V): ZWrite = new ZWrite(execute(f))

    def option(key: String, value: String): ZWrite = chain(_.option(key, value))
    def format(name: String): ZWrite               = chain(_.format(name))
    def mode(saveMode: String): ZWrite             = chain(_.mode(saveMode = saveMode))

    def parquet(path: String): Task[Unit] = execute(_.parquet(path))
    def text(path: String): Task[Unit]    = execute(_.text(path))
    def save(path: String): Task[Unit]    = execute(_.save(path))
  }

  final def write: ZWrite = new ZWrite(execute(_.write))

  final def as[X: Encoder]: Try[ZDataset[X]] = now(_.as[X])

  final def sparkSession: ZSparkSession = nowTotal(_.sparkSession)

  final def col(colName: String): Try[Column] = now(_.col(colName))

  final def apply(colName: String): Try[Column] = now(_.apply(colName))

  final def cache: Task[Unit] = execute(_.cache()).unit

  final def createTempView(viewName: String): Task[Unit] = execute(_.createTempView(viewName))

  final def rdd: ZRDD[T] = nowTotal(_.rdd)

  final def collect(): Task[Seq[T]] = execute(_.collect().toSeq)(Clean.pure)

  def take(n: Int): Task[Seq[T]] = execute(_.take(n).toSeq)(Clean.pure)

  def show: Task[Unit] = execute(_.show())
}

final class ZDataFrame(dataFrame: DataFrame) extends ZDataX(dataFrame) {
  def count: Task[Long]                          = execute(_.count())
  def filter(condition: Column): Try[ZDataFrame] = now(_.filter(condition))def printSchema: Task[Unit]                                = execute(_.printSchema())
  def groupBy(cols: Column*): Try[ZRelationalGroupedDataset] = now(_.groupBy(cols: _*))
}

final class ZDataset[T](dataset: Dataset[T]) extends ZDataX(dataset) {
  def filter(func: T => Boolean): ZDataset[T]          = nowTotal(_.filter(func))
  def map[B: Encoder](f: T => B): ZDataset[B]          = nowTotal(_.map(f))
  def flatMap[B: Encoder](f: T => Seq[B]): ZDataset[B] = nowTotal(_.flatMap(f))
}

case class ZRelationalGroupedDataset(relationalGroupedDataset: RelationalGroupedDataset)
    extends ZWrap(relationalGroupedDataset) {
  def count: ZDataFrame = nowTotal(_.count())
}
