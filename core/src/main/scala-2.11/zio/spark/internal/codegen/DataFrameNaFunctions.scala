/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.internal.codegen

import org.apache.spark.sql.{DataFrame => UnderlyingDataFrame, DataFrameNaFunctions => UnderlyingDataFrameNaFunctions}

import zio.spark.internal.Impure
import zio.spark.internal.Impure.ImpureBox
import zio.spark.sql.{DataFrame, Dataset, TryAnalysis}

case class DataFrameNaFunctions(underlyingDataFrameNaFunctions: ImpureBox[UnderlyingDataFrameNaFunctions])
    extends Impure[UnderlyingDataFrameNaFunctions](underlyingDataFrameNaFunctions) {
  import underlyingDataFrameNaFunctions._

  /**
   * Applies a transformation to the underlying DataFrameNaFunctions.
   */
  def transformation(f: UnderlyingDataFrameNaFunctions => UnderlyingDataFrame): DataFrame =
    succeedNow(f.andThen(x => Dataset(x)))

  /**
   * Applies a transformation to the underlying DataFrameNaFunctions, it
   * is used for transformations that can fail due to an
   * AnalysisException.
   */
  def transformationWithAnalysis(f: UnderlyingDataFrameNaFunctions => UnderlyingDataFrame): TryAnalysis[DataFrame] =
    TryAnalysis(transformation(f))

  /**
   * Returns a new `DataFrame` that drops rows containing any null or
   * NaN values.
   *
   * @since 1.3.1
   */
  final def drop: DataFrame = transformation(_.drop())

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN
   * values.
   *
   * If `how` is "any", then drop rows containing any null or NaN
   * values. If `how` is "all", then drop rows only if every column is
   * null or NaN for that row.
   *
   * @since 1.3.1
   */
  final def drop(how: String): DataFrame = transformation(_.drop(how))

  /**
   * Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values.
   *
   * @since 1.3.1
   */
  final def drop(minNonNulls: Int): DataFrame = transformation(_.drop(minNonNulls))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * numeric columns with `value`.
   *
   * @since 2.1.1
   */
  final def fill(value: Long): DataFrame = transformation(_.fill(value))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * numeric columns with `value`.
   * @since 1.3.1
   */
  final def fill(value: Double): DataFrame = transformation(_.fill(value))

  /**
   * Returns a new `DataFrame` that replaces null values in string
   * columns with `value`.
   *
   * @since 1.3.1
   */
  final def fill(value: String): DataFrame = transformation(_.fill(value))

  /**
   * Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is
   * the replacement value. The value must be of the following type:
   * `Int`, `Long`, `Float`, `Double`, `String`, `Boolean`. Replacement
   * values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with
   * string "unknown", and null values in column "B" with numeric value
   * 1.0.
   * {{{
   *   df.na.fill(Map(
   *     "A" -> "unknown",
   *     "B" -> 1.0
   *   ))
   * }}}
   *
   * @since 1.3.1
   */
  final def fill(valueMap: Map[String, Any]): DataFrame = transformation(_.fill(valueMap))

  // ===============

  /**
   * Returns a new `DataFrame` that drops rows containing any null or
   * NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  final def drop(cols: Seq[String]): TryAnalysis[DataFrame] = transformationWithAnalysis(_.drop(cols))

  /**
   * Returns a new `DataFrame` that drops rows containing null or NaN
   * values in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values
   * in the specified columns. If `how` is "all", then drop rows only if
   * every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  final def drop(how: String, cols: Seq[String]): TryAnalysis[DataFrame] = transformationWithAnalysis(_.drop(how, cols))

  /**
   * Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  final def drop(minNonNulls: Int, cols: Seq[String]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.drop(minNonNulls, cols))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * specified numeric columns. If a specified column is not a numeric
   * column, it is ignored.
   *
   * @since 2.1.1
   */
  final def fill(value: Long, cols: Seq[String]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * specified numeric columns. If a specified column is not a numeric
   * column, it is ignored.
   *
   * @since 1.3.1
   */
  final def fill(value: Double, cols: Seq[String]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null values in specified
   * string columns. If a specified column is not a string column, it is
   * ignored.
   *
   * @since 1.3.1
   */
  final def fill(value: String, cols: Seq[String]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.fill(value, cols))

  /**
   * Replaces values matching keys in `replacement` map. Key and value
   * of `replacement` map must have the same type, and can only be
   * doubles, strings or booleans. If `col` is "*", then the replacement
   * is applied on all string columns , numeric columns or boolean
   * columns.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.na.replace("height", Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.na.replace("name", Map("UNKNOWN" -> "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.na.replace("*", Map("UNKNOWN" -> "unnamed"));
   * }}}
   *
   * @param col
   *   name of the column to apply the value replacement
   * @param replacement
   *   value replacement map, as explained above
   *
   * @since 1.3.1
   */
  final def replace[T](col: String, replacement: Map[T, T]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.replace(col, replacement))

  /**
   * Replaces values matching keys in `replacement` map. Key and value
   * of `replacement` map must have the same type, and can only be
   * doubles , strings or booleans.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.na.replace("height" :: "weight" :: Nil, Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.na.replace("firstname" :: "lastname" :: Nil, Map("UNKNOWN" -> "unnamed"));
   * }}}
   *
   * @param cols
   *   list of columns to apply the value replacement
   * @param replacement
   *   value replacement map, as explained above
   *
   * @since 1.3.1
   */
  final def replace[T](cols: Seq[String], replacement: Map[T, T]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.replace(cols, replacement))

}
