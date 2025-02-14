/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.sql

import org.apache.spark.sql.{
  Column,
  Dataset => UnderlyingDataset,
  Encoder,
  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset,
  TypedColumn
}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

final case class KeyValueGroupedDataset[K, V](underlying: UnderlyingKeyValueGroupedDataset[K, V]) { self =>

  /** Unpack the underlying KeyValueGroupedDataset into a DataFrame. */
  def unpack[U](f: UnderlyingKeyValueGroupedDataset[K, V] => UnderlyingDataset[U]): Dataset[U] = Dataset(f(underlying))

  /**
   * Unpack the underlying KeyValueGroupedDataset into a DataFrame, it
   * is used for transformations that can fail due to an
   * AnalysisException.
   */
  def unpackWithAnalysis[U](
      f: UnderlyingKeyValueGroupedDataset[K, V] => UnderlyingDataset[U]
  ): TryAnalysis[Dataset[U]] = TryAnalysis(unpack(f))

  /**
   * Applies a transformation to the underlying KeyValueGroupedDataset.
   */
  def transformation[KNew, VNew](
      f: UnderlyingKeyValueGroupedDataset[K, V] => UnderlyingKeyValueGroupedDataset[KNew, VNew]
  ): KeyValueGroupedDataset[KNew, VNew] = KeyValueGroupedDataset(f(underlying))

  // Generated functions coming from spark

  def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = transformation(_.keyAs[L])

  def mapValues[W: Encoder](func: V => W): KeyValueGroupedDataset[K, W] = transformation(_.mapValues[W](func))

  // ===============

  def cogroup[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]
  ): Dataset[R] = unpack(_.cogroup[U, R](other.underlying)(f))

  def count: Dataset[(K, Long)] = unpack(_.count())

  def flatMapGroups[U: Encoder](f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] = unpack(_.flatMapGroups[U](f))

  def flatMapGroupsWithState[S: Encoder, U: Encoder](outputMode: OutputMode, timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]
  ): Dataset[U] = unpack(_.flatMapGroupsWithState[S, U](outputMode, timeoutConf)(func))

  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S]
  )(func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] =
    unpack(_.flatMapGroupsWithState[S, U](outputMode, timeoutConf, initialState.underlying)(func))

  def keys: Dataset[K] = unpack(_.keys)

  def mapGroups[U: Encoder](f: (K, Iterator[V]) => U): Dataset[U] = unpack(_.mapGroups[U](f))

  def mapGroupsWithState[S: Encoder, U: Encoder](func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] =
    unpack(_.mapGroupsWithState[S, U](func))

  def mapGroupsWithState[S: Encoder, U: Encoder](timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U
  ): Dataset[U] = unpack(_.mapGroupsWithState[S, U](timeoutConf)(func))

  def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S]
  )(func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] =
    unpack(_.mapGroupsWithState[S, U](timeoutConf, initialState.underlying)(func))

  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = unpack(_.reduceGroups(f))

  // ===============

  def agg[U1](col1: TypedColumn[V, U1]): TryAnalysis[Dataset[(K, U1)]] = unpackWithAnalysis(_.agg[U1](col1))

  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2]): TryAnalysis[Dataset[(K, U1, U2)]] =
    unpackWithAnalysis(_.agg[U1, U2](col1, col2))

  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]
  ): TryAnalysis[Dataset[(K, U1, U2, U3)]] = unpackWithAnalysis(_.agg[U1, U2, U3](col1, col2, col3))

  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]
  ): TryAnalysis[Dataset[(K, U1, U2, U3, U4)]] = unpackWithAnalysis(_.agg[U1, U2, U3, U4](col1, col2, col3, col4))

  def agg[U1, U2, U3, U4, U5](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5]
  ): TryAnalysis[Dataset[(K, U1, U2, U3, U4, U5)]] =
    unpackWithAnalysis(_.agg[U1, U2, U3, U4, U5](col1, col2, col3, col4, col5))

  def agg[U1, U2, U3, U4, U5, U6](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6]
  ): TryAnalysis[Dataset[(K, U1, U2, U3, U4, U5, U6)]] =
    unpackWithAnalysis(_.agg[U1, U2, U3, U4, U5, U6](col1, col2, col3, col4, col5, col6))

  def agg[U1, U2, U3, U4, U5, U6, U7](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7]
  ): TryAnalysis[Dataset[(K, U1, U2, U3, U4, U5, U6, U7)]] =
    unpackWithAnalysis(_.agg[U1, U2, U3, U4, U5, U6, U7](col1, col2, col3, col4, col5, col6, col7))

  def agg[U1, U2, U3, U4, U5, U6, U7, U8](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7],
      col8: TypedColumn[V, U8]
  ): TryAnalysis[Dataset[(K, U1, U2, U3, U4, U5, U6, U7, U8)]] =
    unpackWithAnalysis(_.agg[U1, U2, U3, U4, U5, U6, U7, U8](col1, col2, col3, col4, col5, col6, col7, col8))

  def cogroupSorted[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(
      thisSortExprs: Column*
  )(otherSortExprs: Column*)(f: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): TryAnalysis[Dataset[R]] =
    unpackWithAnalysis(_.cogroupSorted[U, R](other.underlying)(thisSortExprs: _*)(otherSortExprs: _*)(f))

  def flatMapSortedGroups[U: Encoder](sortExprs: Column*)(
      f: (K, Iterator[V]) => IterableOnce[U]
  ): TryAnalysis[Dataset[U]] = unpackWithAnalysis(_.flatMapSortedGroups[U](sortExprs: _*)(f))

  // ===============

  // Ignored methods
  //
  // [[org.apache.spark.sql.KeyValueGroupedDataset.cogroup]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.flatMapGroups]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.flatMapGroupsWithState]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.mapGroups]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.mapGroupsWithState]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.mapValues]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.reduceGroups]]
  // [[org.apache.spark.sql.KeyValueGroupedDataset.toString]]
}
