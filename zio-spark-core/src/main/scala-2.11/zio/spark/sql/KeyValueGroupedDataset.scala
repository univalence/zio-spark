/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.sql

import org.apache.spark.sql.{
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

  // Handmade functions specific to zio-spark

  // Generated functions coming from spark

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the type of the key
   * has been mapped to the specified type. The mapping of key columns
   * to the type follows the same rules as `as` on [[Dataset]].
   *
   * @since 1.6.0
   */
  def keyAs[L: Encoder]: KeyValueGroupedDataset[L, V] = transformation(_.keyAs[L])

  /**
   * Returns a new [[KeyValueGroupedDataset]] where the given function
   * `func` has been applied to the data. The grouping key is unchanged
   * by this.
   *
   * {{{
   *   // Create values grouped by key from a Dataset[(K, V)]
   *   ds.groupByKey(_._1).mapValues(_._2) // Scala
   * }}}
   *
   * @since 2.1.0
   */
  def mapValues[W: Encoder](func: V => W): KeyValueGroupedDataset[K, W] = transformation(_.mapValues[W](func))

  // ===============

  /**
   * (Scala-specific) Applies the given function to each cogrouped data.
   * For each unique group, the function will be passed the grouping key
   * and 2 iterators containing all elements in the group from
   * [[Dataset]] `this` and `other`. The function can return an iterator
   * containing elements of an arbitrary type which will be returned as
   * a new [[Dataset]].
   *
   * @since 1.6.0
   */
  def cogroup[U, R: Encoder](other: KeyValueGroupedDataset[K, U])(
      f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]
  ): Dataset[R] = unpack(_.cogroup[U, R](other.underlying)(f))

  /**
   * Returns a [[Dataset]] that contains a tuple with each key and the
   * number of items present for that key.
   *
   * @since 1.6.0
   */
  def count: Dataset[(K, Long)] = unpack(_.count())

  /**
   * (Scala-specific) Applies the given function to each group of data.
   * For each unique group, the function will be passed the group key
   * and an iterator that contains all of the elements in the group. The
   * function can return an iterator containing elements of an arbitrary
   * type which will be returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result
   * requires shuffling all the data in the [[Dataset]]. If an
   * application intends to perform an aggregation over each key, it is
   * best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given
   * group is too large to fit into memory. However, users must take
   * care to avoid materializing the whole iterator for a group (for
   * example, by calling `toList`) unless they are sure that this is
   * possible given the memory constraints of their cluster.
   *
   * @since 1.6.0
   */
  def flatMapGroups[U: Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = unpack(_.flatMapGroups[U](f))

  /**
   * ::Experimental:: (Scala-specific) Applies the given function to
   * each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by
   * the function. For a static batch Dataset, the function will be
   * invoked once per group. For a streaming Dataset, the function will
   * be invoked for each group repeatedly in every trigger, and updates
   * to each group's state will be saved across invocations. See
   * `GroupState` for more details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark
   *   SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL
   *   types.
   * @param func
   *   Function to be called on every group.
   * @param outputMode
   *   The output mode of the function.
   * @param timeoutConf
   *   Timeout configuration for groups that do not receive data for a
   *   while.
   *
   * See [[Encoder]] for more details on what types are encodable to
   * Spark SQL.
   * @since 2.2.0
   */
  def flatMapGroupsWithState[S: Encoder, U: Encoder](outputMode: OutputMode, timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]
  ): Dataset[U] = unpack(_.flatMapGroupsWithState[S, U](outputMode, timeoutConf)(func))

  /**
   * Returns a [[Dataset]] that contains each unique key. This is
   * equivalent to doing mapping over the Dataset to extract the keys
   * and then running a distinct operation on those.
   *
   * @since 1.6.0
   */
  def keys: Dataset[K] = unpack(_.keys)

  /**
   * (Scala-specific) Applies the given function to each group of data.
   * For each unique group, the function will be passed the group key
   * and an iterator that contains all of the elements in the group. The
   * function can return an element of arbitrary type which will be
   * returned as a new [[Dataset]].
   *
   * This function does not support partial aggregation, and as a result
   * requires shuffling all the data in the [[Dataset]]. If an
   * application intends to perform an aggregation over each key, it is
   * best to use the reduce function or an
   * `org.apache.spark.sql.expressions#Aggregator`.
   *
   * Internally, the implementation will spill to disk if any given
   * group is too large to fit into memory. However, users must take
   * care to avoid materializing the whole iterator for a group (for
   * example, by calling `toList`) unless they are sure that this is
   * possible given the memory constraints of their cluster.
   *
   * @since 1.6.0
   */
  def mapGroups[U: Encoder](f: (K, Iterator[V]) => U): Dataset[U] = unpack(_.mapGroups[U](f))

  /**
   * ::Experimental:: (Scala-specific) Applies the given function to
   * each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by
   * the function. For a static batch Dataset, the function will be
   * invoked once per group. For a streaming Dataset, the function will
   * be invoked for each group repeatedly in every trigger, and updates
   * to each group's state will be saved across invocations. See
   * [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark
   *   SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL
   *   types.
   * @param func
   *   Function to be called on every group.
   *
   * See [[Encoder]] for more details on what types are encodable to
   * Spark SQL.
   * @since 2.2.0
   */
  def mapGroupsWithState[S: Encoder, U: Encoder](func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] =
    unpack(_.mapGroupsWithState[S, U](func))

  /**
   * ::Experimental:: (Scala-specific) Applies the given function to
   * each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by
   * the function. For a static batch Dataset, the function will be
   * invoked once per group. For a streaming Dataset, the function will
   * be invoked for each group repeatedly in every trigger, and updates
   * to each group's state will be saved across invocations. See
   * [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S
   *   The type of the user-defined state. Must be encodable to Spark
   *   SQL types.
   * @tparam U
   *   The type of the output objects. Must be encodable to Spark SQL
   *   types.
   * @param func
   *   Function to be called on every group.
   * @param timeoutConf
   *   Timeout configuration for groups that do not receive data for a
   *   while.
   *
   * See [[Encoder]] for more details on what types are encodable to
   * Spark SQL.
   * @since 2.2.0
   */
  def mapGroupsWithState[S: Encoder, U: Encoder](timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U
  ): Dataset[U] = unpack(_.mapGroupsWithState[S, U](timeoutConf)(func))

  /**
   * (Scala-specific) Reduces the elements of each group of data using
   * the specified binary function. The given function must be
   * commutative and associative or the result may be non-deterministic.
   *
   * @since 1.6.0
   */
  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] = unpack(_.reduceGroups(f))

  // ===============

  /**
   * Computes the given aggregation, returning a [[Dataset]] of tuples
   * for each unique key and the result of computing this aggregation
   * over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1](col1: TypedColumn[V, U1]): TryAnalysis[Dataset[(K, U1)]] = unpackWithAnalysis(_.agg[U1](col1))

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples
   * for each unique key and the result of computing these aggregations
   * over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2]): TryAnalysis[Dataset[(K, U1, U2)]] =
    unpackWithAnalysis(_.agg[U1, U2](col1, col2))

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples
   * for each unique key and the result of computing these aggregations
   * over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]
  ): TryAnalysis[Dataset[(K, U1, U2, U3)]] = unpackWithAnalysis(_.agg[U1, U2, U3](col1, col2, col3))

  /**
   * Computes the given aggregations, returning a [[Dataset]] of tuples
   * for each unique key and the result of computing these aggregations
   * over all elements in the group.
   *
   * @since 1.6.0
   */
  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]
  ): TryAnalysis[Dataset[(K, U1, U2, U3, U4)]] = unpackWithAnalysis(_.agg[U1, U2, U3, U4](col1, col2, col3, col4))

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
