/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.sql

import org.apache.spark.sql.{
  Column,
  DataFrame => UnderlyingDataFrame,
  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset
}

final case class RelationalGroupedDataset(underlyingRelationalGroupedDataset: UnderlyingRelationalGroupedDataset) {
  self =>

  /**
   * Unpack the underlying RelationalGroupedDataset into a DataFrame.
   */
  def unpack(f: UnderlyingRelationalGroupedDataset => UnderlyingDataFrame): DataFrame =
    Dataset(f(underlyingRelationalGroupedDataset))

  /**
   * Unpack the underlying RelationalGroupedDataset into a DataFrame, it
   * is used for transformations that can fail due to an
   * AnalysisException.
   */
  def unpackWithAnalysis(f: UnderlyingRelationalGroupedDataset => UnderlyingDataFrame): TryAnalysis[DataFrame] =
    TryAnalysis(unpack(f))

  /**
   * Applies a transformation to the underlying
   * RelationalGroupedDataset.
   */
  def transformation(
      f: UnderlyingRelationalGroupedDataset => UnderlyingRelationalGroupedDataset
  ): RelationalGroupedDataset = RelationalGroupedDataset(f(underlyingRelationalGroupedDataset))

  /**
   * Applies a transformation to the underlying
   * RelationalGroupedDataset, it is used for transformations that can
   * fail due to an AnalysisException.
   */
  def transformationWithAnalysis(
      f: UnderlyingRelationalGroupedDataset => UnderlyingRelationalGroupedDataset
  ): TryAnalysis[RelationalGroupedDataset] = TryAnalysis(transformation(f))

  /** Applies an action to the underlying RelationalGroupedDataset. */
  def get[U](f: UnderlyingRelationalGroupedDataset => U): U = f(underlyingRelationalGroupedDataset)

  /**
   * Applies an action to the underlying RelationalGroupedDataset, it is
   * used for transformations that can fail due to an AnalysisException.
   */
  def getWithAnalysis[U](f: UnderlyingRelationalGroupedDataset => U): TryAnalysis[U] =
    TryAnalysis(f(underlyingRelationalGroupedDataset))

  // Handmade functions specific to zio-spark

  // Generated functions coming from spark

  /**
   * Pivots a column of the current `DataFrame` and performs the
   * specified aggregation.
   *
   * There are two versions of `pivot` function: one that requires the
   * caller to specify the list of distinct values to pivot on, and one
   * that does not. The latter is more concise but less efficient,
   * because Spark needs to first compute the list of distinct values
   * internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings")
   * }}}
   *
   * @param pivotColumn
   *   Name of the column to pivot.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String): TryAnalysis[RelationalGroupedDataset] =
    transformationWithAnalysis(_.pivot(pivotColumn))

  /**
   * Pivots a column of the current `DataFrame` and performs the
   * specified aggregation. There are two versions of pivot function:
   * one that requires the caller to specify the list of distinct values
   * to pivot on, and one that does not. The latter is more concise but
   * less efficient, because Spark needs to first compute the list of
   * distinct values internally.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy("year").pivot("course", Seq("dotNET", "Java")).sum("earnings")
   *
   *   // Or without specifying column values (less efficient)
   *   df.groupBy("year").pivot("course").sum("earnings")
   * }}}
   *
   * @param pivotColumn
   *   Name of the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output
   *   DataFrame.
   * @since 1.6.0
   */
  def pivot(pivotColumn: String, values: Seq[Any]): TryAnalysis[RelationalGroupedDataset] =
    transformationWithAnalysis(_.pivot(pivotColumn, values))

  /**
   * Pivots a column of the current `DataFrame` and performs the
   * specified aggregation. This is an overloaded version of the `pivot`
   * method with `pivotColumn` of the `String` type.
   *
   * {{{
   *   // Or without specifying column values (less efficient)
   *   df.groupBy($"year").pivot($"course").sum($"earnings");
   * }}}
   *
   * @param pivotColumn
   *   he column to pivot.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column): TryAnalysis[RelationalGroupedDataset] =
    transformationWithAnalysis(_.pivot(pivotColumn))

  /**
   * Pivots a column of the current `DataFrame` and performs the
   * specified aggregation. This is an overloaded version of the `pivot`
   * method with `pivotColumn` of the `String` type.
   *
   * {{{
   *   // Compute the sum of earnings for each year by course with each course as a separate column
   *   df.groupBy($"year").pivot($"course", Seq("dotNET", "Java")).sum($"earnings")
   * }}}
   *
   * @param pivotColumn
   *   the column to pivot.
   * @param values
   *   List of values that will be translated to columns in the output
   *   DataFrame.
   * @since 2.4.0
   */
  def pivot(pivotColumn: Column, values: Seq[Any]): TryAnalysis[RelationalGroupedDataset] =
    transformationWithAnalysis(_.pivot(pivotColumn, values))

  // ===============

  /**
   * Count the number of rows for each group. The resulting `DataFrame`
   * will also contain the grouping columns.
   *
   * @since 1.3.0
   */
  def count: DataFrame = unpack(_.count())

  // ===============

  /**
   * Compute aggregates by specifying the column names and aggregate
   * methods. The resulting `DataFrame` will also contain the grouping
   * columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`,
   * `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   )
   * }}}
   *
   * @since 1.3.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): TryAnalysis[DataFrame] =
    unpackWithAnalysis(_.agg(aggExpr, aggExprs: _*))

  /**
   * Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting `DataFrame` will also contain the
   * grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`,
   * `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   ))
   * }}}
   *
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): TryAnalysis[DataFrame] = unpackWithAnalysis(_.agg(exprs))

  /**
   * Compute aggregates by specifying a series of aggregate columns.
   * Note that this function by default retains the grouping columns in
   * its output. To not retain grouping columns, set
   * `spark.sql.retainGroupColumns` to false.
   *
   * The available aggregate methods are defined in
   * [[org.apache.spark.sql.functions]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * }}}
   *
   * Note that before Spark 1.4, the default behavior is to NOT retain
   * grouping columns. To change to that behavior, set config variable
   * `spark.sql.retainGroupColumns` to `false`.
   * {{{
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * }}}
   *
   * @since 1.3.0
   */
  def agg(expr: Column, exprs: Column*): TryAnalysis[DataFrame] = unpackWithAnalysis(_.agg(expr, exprs: _*))

  /**
   * Compute the mean value for each numeric columns for each group. The
   * resulting `DataFrame` will also contain the grouping columns. When
   * specified columns are given, only compute the mean values for them.
   *
   * @since 1.3.0
   */
  def avg(colNames: String*): TryAnalysis[DataFrame] = unpackWithAnalysis(_.avg(colNames: _*))

  /**
   * Compute the max value for each numeric columns for each group. The
   * resulting `DataFrame` will also contain the grouping columns. When
   * specified columns are given, only compute the max values for them.
   *
   * @since 1.3.0
   */
  def max(colNames: String*): TryAnalysis[DataFrame] = unpackWithAnalysis(_.max(colNames: _*))

  /**
   * Compute the average value for each numeric columns for each group.
   * This is an alias for `avg`. The resulting `DataFrame` will also
   * contain the grouping columns. When specified columns are given,
   * only compute the average values for them.
   *
   * @since 1.3.0
   */
  def mean(colNames: String*): TryAnalysis[DataFrame] = unpackWithAnalysis(_.mean(colNames: _*))

  /**
   * Compute the min value for each numeric column for each group. The
   * resulting `DataFrame` will also contain the grouping columns. When
   * specified columns are given, only compute the min values for them.
   *
   * @since 1.3.0
   */
  def min(colNames: String*): TryAnalysis[DataFrame] = unpackWithAnalysis(_.min(colNames: _*))

  /**
   * Compute the sum for each numeric columns for each group. The
   * resulting `DataFrame` will also contain the grouping columns. When
   * specified columns are given, only compute the sum for them.
   *
   * @since 1.3.0
   */
  def sum(colNames: String*): TryAnalysis[DataFrame] = unpackWithAnalysis(_.sum(colNames: _*))

  // ===============

  // Ignored methods
  //
  // [[org.apache.spark.sql.RelationalGroupedDataset.toString]]

}
