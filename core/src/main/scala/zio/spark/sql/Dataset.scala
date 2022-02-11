package zio.spark.sql

import org.apache.spark.sql.{Column, Dataset => UnderlyingDataset, Encoder, Row}
import org.apache.spark.storage.StorageLevel

import zio.{Task, UIO}
import zio.spark.impure.Impure.ImpureBox
import zio.spark.rdd.RDD

final case class Dataset[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
    extends ExtraDatasetFeature[T](underlyingDataset) {
  import underlyingDataset._

  /**
   * Returns the columns of the Dataset.
   *
   * See [[UnderlyingDataset.columns]] for more information.
   */
  def columns: Seq[String] = underlyingDataset.succeedNow(_.columns.toSeq)

  /**
   * Maps each record to the specified type.
   *
   * See [[UnderlyingDataset.as]] for more information.
   */
  def as[U: Encoder]: TryAnalysis[Dataset[U]] = transformationWithAnalysis(_.as[U])

  /** Applies a transformation to the underlying dataset. */
  def transformation[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): Dataset[U] =
    succeedNow(f.andThen(x => Dataset(x)))

  /**
   * Applies a transformation to the underlying dataset, it is used for
   * transformations that can fail due to an AnalysisException.
   */
  def transformationWithAnalysis[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
    TryAnalysis(transformation(f))

  /** Applies an action to the underlying dataset. */
  def action[A](f: UnderlyingDataset[T] => A): Task[A] = attemptBlocking(f)

  /**
   * A variant of select that accepts SQL expressions.
   *
   * See [[UnderlyingDataset.selectExpr]] for more information.
   */
  def selectExpr(exprs: String*): TryAnalysis[Dataset[Row]] = transformationWithAnalysis(_.selectExpr(exprs: _*))

  /**
   * Limits the number of rows of a dataset.
   *
   * See [[UnderlyingDataset.limit]] for more information.
   */
  def limit(n: Int): Dataset[T] = transformation(_.limit(n))

  /**
   * Returns a new Dataset that only contains elements respecting the
   * predicate.
   *
   * See [[UnderlyingDataset.filter]] for more information.
   */
  def filter(f: T => Boolean): Dataset[T] = transformation(_.filter(f))

  def filter(expr: String): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.filter(expr))

  /**
   * Applies the function f to each record of the dataset.
   *
   * See [[UnderlyingDataset.map]] for more information.
   */
  def map[U: Encoder](f: T => U): Dataset[U] = transformation(_.map(f))

  /**
   * Applies the function f to each record of the dataset and then
   * flattening the result.
   *
   * See [[UnderlyingDataset.flatMap]] for more information.
   */
  def flatMap[U: Encoder](f: T => Iterable[U]): Dataset[U] = transformation(_.flatMap(f))

  /**
   * Counts the number of rows of a dataset.
   *
   * See [[UnderlyingDataset.count]] for more information.
   */
  def count: Task[Long] = action(_.count())

  /**
   * Retrieves the rows of a dataset as a list of elements.
   *
   * See [[UnderlyingDataset.collect]] for more information.
   */
  def collect: Task[Seq[T]] = action(_.collect().toSeq)

  /** Alias for [[head]]. */
  def first: Task[T] = head

  /**
   * Takes the first element of a dataset.
   *
   * See [[UnderlyingDataset.head]] for more information.
   */
  def head: Task[T] = head(1).map(_.head)

  /** Alias for [[head]]. */
  def take(n: Int): Task[List[T]] = head(n)

  /** Alias for [[headOption]]. */
  def firstOption: Task[Option[T]] = headOption

  /** Takes the first element of a dataset or None. */
  def headOption: Task[Option[T]] = head(1).map(_.headOption)

  /**
   * Takes the n elements of a dataset.
   *
   * See [[UnderlyingDataset.head]] for more information.
   */
  def head(n: Int): Task[List[T]] = action(_.head(n).toList)

  /**
   * Transform the dataset into a [[RDD]].
   *
   * See [[UnderlyingDataset.rdd]] for more information.
   */
  def rdd: RDD[T] = RDD(succeedNow(_.rdd))

  /**
   * Chains custom transformations.
   *
   * See [[UnderlyingDataset.transform]] for more information.
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /**
   * Returns a new Dataset that contains only the unique rows from this
   * Dataset, considering only the subset of columns.
   *
   * See [[UnderlyingDataset.dropDuplicates]] for more information.
   */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = transformation(_.dropDuplicates(colNames))

  /**
   * Returns a new Dataset that contains only the unique rows from this
   * Dataset, considering only the subset of columns.
   *
   * See [[UnderlyingDataset.dropDuplicates]] for more information.
   */
  def dropDuplicates(colNames: String*)(implicit d: DummyImplicit): Dataset[T] = dropDuplicates(colNames)

  /**
   * Returns a new Dataset with a column dropped if it exists.
   *
   * See [[UnderlyingDataset.drop]] for more information.
   */
  def drop(colName: String): DataFrame = drop(Seq(colName): _*)

  /**
   * Returns a new Dataset with a column dropped if it exists.
   *
   * See [[UnderlyingDataset.drop]] for more information.
   */
  def drop(col: Column): DataFrame = transformation(_.drop(col))

  /**
   * Returns a new Dataset with columns dropped if they exist.
   *
   * See [[UnderlyingDataset.drop]] for more information.
   */
  def drop(colNames: String*): DataFrame = transformation(_.drop(colNames: _*))

  /**
   * Returns a new Dataset that contains only the unique rows from this
   * Dataset, considering all columns.
   *
   * See [[UnderlyingDataset.dropDuplicates]] for more information.
   */
  def dropDuplicates: Dataset[T] = dropDuplicates(this.columns)

  /** Alias for [[dropDuplicates]]. */
  def distinct: Dataset[T] = dropDuplicates

  /**
   * Creates a local temporary view using the given name.
   *
   * See [[UnderlyingDataset.createOrReplaceTempView]] for more
   * information.
   *
   * TODO : Change to IO[AnalysisError, Unit]
   */
  def createOrReplaceTempView(viewName: String): Task[Unit] = attemptBlocking(_.createOrReplaceTempView(viewName))

  /**
   * Persist this Dataset with the given storage level.
   *
   * See [[UnderlyingDataset.persist]] for more information.
   */
  def persist(storageLevel: StorageLevel): UIO[Dataset[T]] = succeed(ds => Dataset(ds.persist(storageLevel)))

  /**
   * Persist this Dataset with the default storage level.
   *
   * See [[UnderlyingDataset.persist]] for more information.
   */
  def persist: UIO[Dataset[T]] = persist(StorageLevel.MEMORY_AND_DISK)

  /** Alias for [[persist]]. */
  def cache: UIO[Dataset[T]] = persist

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it
   * from memory and disk in a blocking way.
   *
   * See [[UnderlyingDataset.unpersist]] for more information.
   */
  def unpersistBlocking: UIO[Dataset[T]] = succeed(ds => Dataset(ds.unpersist(blocking = true)))

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it
   * from memory and disk.
   *
   * See [[UnderlyingDataset.unpersist]] for more information.
   */
  def unpersist: UIO[Dataset[T]] = succeed(ds => Dataset(ds.unpersist(blocking = false)))

  /**
   * Get the Dataset's current storage level, or StorageLevel.NONE if
   * not persisted.
   *
   * See [[UnderlyingDataset.storageLevel]] for more information.
   */
  def storageLevel: UIO[StorageLevel] = succeed(_.storageLevel)
}
