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
  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions
}
import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}

final case class DataFrameStatFunctions(underlyingDataFrameStatFunctions: UnderlyingDataFrameStatFunctions) { self =>

  /** Applies an action to the underlying DataFrameStatFunctions. */
  def get[U](f: UnderlyingDataFrameStatFunctions => U): U = f(underlyingDataFrameStatFunctions)

  /** Wraps a function into a TryAnalysis. */
  def getWithAnalysis[U](f: UnderlyingDataFrameStatFunctions => U): TryAnalysis[U] = TryAnalysis(get(f))

  /**
   * Applies a transformation to the underlying DataFrameStatFunctions.
   */
  def transformation(f: UnderlyingDataFrameStatFunctions => UnderlyingDataFrame): DataFrame =
    Dataset(f(underlyingDataFrameStatFunctions))

  /**
   * Applies a transformation to the underlying DataFrameStatFunctions,
   * it is used for transformations that can fail due to an
   * AnalysisException.
   */
  def transformationWithAnalysis(f: UnderlyingDataFrameStatFunctions => UnderlyingDataFrame): TryAnalysis[DataFrame] =
    TryAnalysis(transformation(f))

  // Handmade functions specific to zio-spark

  // Generated functions coming from spark

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param colName
   *   name of the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param fpp
   *   expected false positive probability of the filter.
   * @since 2.0.0
   */
  def bloomFilter(colName: String, expectedNumItems: Long, fpp: Double): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(colName, expectedNumItems, fpp))

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param col
   *   the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param fpp
   *   expected false positive probability of the filter.
   * @since 2.0.0
   */
  def bloomFilter(col: Column, expectedNumItems: Long, fpp: Double): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(col, expectedNumItems, fpp))

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param colName
   *   name of the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param numBits
   *   expected number of bits of the filter.
   * @since 2.0.0
   */
  def bloomFilter(colName: String, expectedNumItems: Long, numBits: Long): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(colName, expectedNumItems, numBits))

  /**
   * Builds a Bloom filter over a specified column.
   *
   * @param col
   *   the column over which the filter is built
   * @param expectedNumItems
   *   expected number of items which will be put into the filter.
   * @param numBits
   *   expected number of bits of the filter.
   * @since 2.0.0
   */
  def bloomFilter(col: Column, expectedNumItems: Long, numBits: Long): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(col, expectedNumItems, numBits))

  /**
   * Calculates the correlation of two columns of a DataFrame. Currently
   * only supports the Pearson Correlation Coefficient. For Spearman
   * Correlation, consider using RDD methods found in MLlib's
   * Statistics.
   *
   * @param col1
   *   the name of the column
   * @param col2
   *   the name of the column to calculate the correlation against
   * @return
   *   The Pearson Correlation Coefficient as a Double.
   *
   * {{{
   *     val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *       .withColumn("rand2", rand(seed=27))
   *     df.stat.corr("rand1", "rand2")
   *     res1: Double = 0.613...
   * }}}
   *
   * @since 1.4.0
   */
  def corr(col1: String, col2: String, method: String): TryAnalysis[Double] =
    getWithAnalysis(_.corr(col1, col2, method))

  /**
   * Calculates the Pearson Correlation Coefficient of two columns of a
   * DataFrame.
   *
   * @param col1
   *   the name of the column
   * @param col2
   *   the name of the column to calculate the correlation against
   * @return
   *   The Pearson Correlation Coefficient as a Double.
   *
   * {{{
   *     val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *       .withColumn("rand2", rand(seed=27))
   *     df.stat.corr("rand1", "rand2", "pearson")
   *     res1: Double = 0.613...
   * }}}
   *
   * @since 1.4.0
   */
  def corr(col1: String, col2: String): TryAnalysis[Double] = getWithAnalysis(_.corr(col1, col2))

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param colName
   *   name of the column over which the sketch is built
   * @param depth
   *   depth of the sketch
   * @param width
   *   width of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(colName: String, depth: Int, width: Int, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(colName, depth, width, seed))

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param colName
   *   name of the column over which the sketch is built
   * @param eps
   *   relative error of the sketch
   * @param confidence
   *   confidence of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(colName: String, eps: Double, confidence: Double, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(colName, eps, confidence, seed))

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param col
   *   the column over which the sketch is built
   * @param depth
   *   depth of the sketch
   * @param width
   *   width of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(col: Column, depth: Int, width: Int, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(col, depth, width, seed))

  /**
   * Builds a Count-min Sketch over a specified column.
   *
   * @param col
   *   the column over which the sketch is built
   * @param eps
   *   relative error of the sketch
   * @param confidence
   *   confidence of the sketch
   * @param seed
   *   random seed
   * @return
   *   a `CountMinSketch` over column `colName`
   * @since 2.0.0
   */
  def countMinSketch(col: Column, eps: Double, confidence: Double, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(col, eps, confidence, seed))

  /**
   * Calculate the sample covariance of two numerical columns of a
   * DataFrame.
   * @param col1
   *   the name of the first column
   * @param col2
   *   the name of the second column
   * @return
   *   the covariance of the two columns.
   *
   * {{{
   *     val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *       .withColumn("rand2", rand(seed=27))
   *     df.stat.cov("rand1", "rand2")
   *     res1: Double = 0.065...
   * }}}
   *
   * @since 1.4.0
   */
  def cov(col1: String, col2: String): TryAnalysis[Double] = getWithAnalysis(_.cov(col1, col2))

  // ===============

  /**
   * Computes a pair-wise frequency table of the given columns. Also
   * known as a contingency table. The number of distinct values for
   * each column should be less than 1e4. At most 1e6 non-zero pair
   * frequencies will be returned. The first column of each row will be
   * the distinct values of `col1` and the column names will be the
   * distinct values of `col2`. The name of the first column will be
   * `col1_col2`. Counts will be returned as `Long`s. Pairs that have no
   * occurrences will have zero as their counts. Null elements will be
   * replaced by "null", and back ticks will be dropped from elements if
   * they exist.
   *
   * @param col1
   *   The name of the first column. Distinct items will make the first
   *   item of each row.
   * @param col2
   *   The name of the second column. Distinct items will make the
   *   column names of the DataFrame.
   * @return
   *   A DataFrame containing for the contingency table.
   *
   * {{{
   *     val df = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3)))
   *       .toDF("key", "value")
   *     val ct = df.stat.crosstab("key", "value")
   *     ct.show()
   *     +---------+---+---+---+
   *     |key_value|  1|  2|  3|
   *     +---------+---+---+---+
   *     |        2|  2|  0|  1|
   *     |        1|  1|  1|  0|
   *     |        3|  0|  1|  1|
   *     +---------+---+---+---+
   * }}}
   *
   * @since 1.4.0
   */
  def crosstab(col1: String, col2: String): TryAnalysis[DataFrame] = transformationWithAnalysis(_.crosstab(col1, col2))

  /**
   * Finding frequent items for columns, possibly with false positives.
   * Using the frequent element count algorithm described in <a
   * href="https://doi.org/10.1145/762471.762473">here</a>, proposed by
   * Karp, Schenker, and Papadimitriou.
   *
   * This function is meant for exploratory data analysis, as we make no
   * guarantee about the backward compatibility of the schema of the
   * resulting `DataFrame`.
   *
   * @param cols
   *   the names of the columns to search frequent items in.
   * @return
   *   A Local DataFrame with the Array of frequent items for each
   *   column.
   *
   * {{{
   *     val rows = Seq.tabulate(100) { i =>
   *       if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *     }
   *     val df = spark.createDataFrame(rows).toDF("a", "b")
   *     // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
   *     // "a" and "b"
   *     val freqSingles = df.stat.freqItems(Seq("a", "b"), 0.4)
   *     freqSingles.show()
   *     +-----------+-------------+
   *     |a_freqItems|  b_freqItems|
   *     +-----------+-------------+
   *     |    [1, 99]|[-1.0, -99.0]|
   *     +-----------+-------------+
   *     // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
   *     val pairDf = df.select(struct("a", "b").as("a-b"))
   *     val freqPairs = pairDf.stat.freqItems(Seq("a-b"), 0.1)
   *     freqPairs.select(explode($"a-b_freqItems").as("freq_ab")).show()
   *     +----------+
   *     |   freq_ab|
   *     +----------+
   *     |  [1,-1.0]|
   *     |   ...    |
   *     +----------+
   * }}}
   *
   * @since 1.4.0
   */
  def freqItems(cols: Seq[String], support: Double): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.freqItems(cols, support))

  /**
   * Finding frequent items for columns, possibly with false positives.
   * Using the frequent element count algorithm described in <a
   * href="https://doi.org/10.1145/762471.762473">here</a>, proposed by
   * Karp, Schenker, and Papadimitriou. Uses a `default` support of 1%.
   *
   * This function is meant for exploratory data analysis, as we make no
   * guarantee about the backward compatibility of the schema of the
   * resulting `DataFrame`.
   *
   * @param cols
   *   the names of the columns to search frequent items in.
   * @return
   *   A Local DataFrame with the Array of frequent items for each
   *   column.
   *
   * @since 1.4.0
   */
  def freqItems(cols: Seq[String]): TryAnalysis[DataFrame] = transformationWithAnalysis(_.freqItems(cols))

  /**
   * Returns a stratified sample without replacement based on the
   * fraction given on each stratum.
   * @param col
   *   column that defines strata
   * @param fractions
   *   sampling fraction for each stratum. If a stratum is not
   *   specified, we treat its fraction as zero.
   * @param seed
   *   random seed
   * @tparam T
   *   stratum type
   * @return
   *   a new `DataFrame` that represents the stratified sample
   *
   * {{{
   *     val df = spark.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
   *       (3, 3))).toDF("key", "value")
   *     val fractions = Map(1 -> 1.0, 3 -> 0.5)
   *     df.stat.sampleBy("key", fractions, 36L).show()
   *     +---+-----+
   *     |key|value|
   *     +---+-----+
   *     |  1|    1|
   *     |  1|    2|
   *     |  3|    2|
   *     +---+-----+
   * }}}
   *
   * @since 1.5.0
   */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.sampleBy(col, fractions, seed))

  /**
   * Returns a stratified sample without replacement based on the
   * fraction given on each stratum.
   * @param col
   *   column that defines strata
   * @param fractions
   *   sampling fraction for each stratum. If a stratum is not
   *   specified, we treat its fraction as zero.
   * @param seed
   *   random seed
   * @tparam T
   *   stratum type
   * @return
   *   a new `DataFrame` that represents the stratified sample
   *
   * The stratified sample can be performed over multiple columns:
   * {{{
   *     import org.apache.spark.sql.Row
   *     import org.apache.spark.sql.functions.struct
   *
   *     val df = spark.createDataFrame(Seq(("Bob", 17), ("Alice", 10), ("Nico", 8), ("Bob", 17),
   *       ("Alice", 10))).toDF("name", "age")
   *     val fractions = Map(Row("Alice", 10) -> 0.3, Row("Nico", 8) -> 1.0)
   *     df.stat.sampleBy(struct($"name", $"age"), fractions, 36L).show()
   *     +-----+---+
   *     | name|age|
   *     +-----+---+
   *     | Nico|  8|
   *     |Alice| 10|
   *     +-----+---+
   * }}}
   *
   * @since 3.0.0
   */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.sampleBy(col, fractions, seed))

}
