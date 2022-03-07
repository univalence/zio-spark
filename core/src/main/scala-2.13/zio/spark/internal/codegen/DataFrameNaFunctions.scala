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
import zio.spark.sql.{DataFrame, Dataset}

case class DataFrameNaFunctions(underlyingDataFrameNaFunctions: ImpureBox[UnderlyingDataFrameNaFunctions])
    extends Impure[UnderlyingDataFrameNaFunctions](underlyingDataFrameNaFunctions) {
  import underlyingDataFrameNaFunctions._

  // scalafix:off

  // scalafix:on

  /**
   * Applies a transformation to the underlying DataFrameNaFunctions.
   */
  def transformation(f: UnderlyingDataFrameNaFunctions => UnderlyingDataFrame): DataFrame =
    succeedNow(f.andThen(x => Dataset(x)))

  /**
   * Returns a new `DataFrame` that drops rows containing any null or
   * NaN values.
   *
   * @since 1.3.1
   */
  def drop: DataFrame = transformation(_.drop())

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
  def drop(how: String): DataFrame = transformation(_.drop(how))

  /**
   * Returns a new `DataFrame` that drops rows containing any null or
   * NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Array[String]): DataFrame = transformation(_.drop(cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows
   * containing any null or NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(cols: Seq[String]): DataFrame = transformation(_.drop(cols))

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
  def drop(how: String, cols: Array[String]): DataFrame = transformation(_.drop(how, cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows
   * containing null or NaN values in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values
   * in the specified columns. If `how` is "all", then drop rows only if
   * every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Seq[String]): DataFrame = transformation(_.drop(how, cols))

  /**
   * Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int): DataFrame = transformation(_.drop(minNonNulls))

  /**
   * Returns a new `DataFrame` that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame = transformation(_.drop(minNonNulls, cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that drops rows
   * containing less than `minNonNulls` non-null and non-NaN values in
   * the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = transformation(_.drop(minNonNulls, cols))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * numeric columns with `value`.
   *
   * @since 2.2.0
   */
  def fill(value: Long): DataFrame = transformation(_.fill(value))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * numeric columns with `value`.
   * @since 1.3.1
   */
  def fill(value: Double): DataFrame = transformation(_.fill(value))

  /**
   * Returns a new `DataFrame` that replaces null values in string
   * columns with `value`.
   *
   * @since 1.3.1
   */
  def fill(value: String): DataFrame = transformation(_.fill(value))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * specified numeric columns. If a specified column is not a numeric
   * column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Array[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null or NaN values in
   * specified numeric columns. If a specified column is not a numeric
   * column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Array[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or
   * NaN values in specified numeric columns. If a specified column is
   * not a numeric column, it is ignored.
   *
   * @since 2.2.0
   */
  def fill(value: Long, cols: Seq[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null or
   * NaN values in specified numeric columns. If a specified column is
   * not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null values in specified
   * string columns. If a specified column is not a string column, it is
   * ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Array[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null
   * values in specified string columns. If a specified column is not a
   * string column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: String, cols: Seq[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null values in boolean
   * columns with `value`.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean): DataFrame = transformation(_.fill(value))

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null
   * values in specified boolean columns. If a specified column is not a
   * boolean column, it is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null values in specified
   * boolean columns. If a specified column is not a boolean column, it
   * is ignored.
   *
   * @since 2.3.0
   */
  def fill(value: Boolean, cols: Array[String]): DataFrame = transformation(_.fill(value, cols))

  /**
   * Returns a new `DataFrame` that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is
   * the replacement value. The value must be of the following type:
   * `Integer`, `Long`, `Float`, `Double`, `String`, `Boolean`.
   * Replacement values are cast to the column data type.
   *
   * For example, the following replaces null values in column "A" with
   * string "unknown", and null values in column "B" with numeric value
   * 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   *
   * @since 1.3.1
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = transformation(_.fill(valueMap))

  /**
   * (Scala-specific) Returns a new `DataFrame` that replaces null
   * values.
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
  def fill(valueMap: Map[String, Any]): DataFrame = transformation(_.fill(valueMap))

  /**
   * Replaces values matching keys in `replacement` map with the
   * corresponding values.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.na.replace("height", ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.na.replace("name", ImmutableMap.of("UNKNOWN", "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.na.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param col
   *   name of the column to apply the value replacement. If `col` is
   *   "*", replacement is applied on all string, numeric or boolean
   *   columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must
   *   have the same type, and can only be doubles, strings or booleans.
   *   The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](col: String, replacement: java.util.Map[T, T]): DataFrame = transformation(_.replace(col, replacement))

  /**
   * Replaces values matching keys in `replacement` map with the
   * corresponding values.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.na.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.na.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param cols
   *   list of columns to apply the value replacement. If `col` is "*",
   *   replacement is applied on all string, numeric or boolean columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must
   *   have the same type, and can only be doubles, strings or booleans.
   *   The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Array[String], replacement: java.util.Map[T, T]): DataFrame =
    transformation(_.replace(cols, replacement))

  /**
   * (Scala-specific) Replaces values matching keys in `replacement`
   * map.
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
   *   name of the column to apply the value replacement. If `col` is
   *   "*", replacement is applied on all string, numeric or boolean
   *   columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must
   *   have the same type, and can only be doubles, strings or booleans.
   *   The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = transformation(_.replace(col, replacement))

  /**
   * (Scala-specific) Replaces values matching keys in `replacement`
   * map.
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
   *   list of columns to apply the value replacement. If `col` is "*",
   *   replacement is applied on all string, numeric or boolean columns.
   * @param replacement
   *   value replacement map. Key and value of `replacement` map must
   *   have the same type, and can only be doubles, strings or booleans.
   *   The map value can have nulls.
   *
   * @since 1.3.1
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = transformation(_.replace(cols, replacement))

}
