/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.sql

import org.apache.spark.sql.{
  Column,
  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions,
  Dataset => UnderlyingDataset
}
import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}

final case class DataFrameStatFunctions(underlying: UnderlyingDataFrameStatFunctions) { self =>

  /** Unpack the underlying DataFrameStatFunctions into a DataFrame. */
  def unpack[U](f: UnderlyingDataFrameStatFunctions => UnderlyingDataset[U]): Dataset[U] = Dataset(f(underlying))

  /**
   * Unpack the underlying DataFrameStatFunctions into a DataFrame, it
   * is used for transformations that can fail due to an
   * AnalysisException.
   */
  def unpackWithAnalysis[U](f: UnderlyingDataFrameStatFunctions => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
    TryAnalysis(unpack(f))

  /**
   * Applies a transformation to the underlying DataFrameStatFunctions.
   */
  def transformation(f: UnderlyingDataFrameStatFunctions => UnderlyingDataFrameStatFunctions): DataFrameStatFunctions =
    DataFrameStatFunctions(f(underlying))

  /**
   * Applies a transformation to the underlying DataFrameStatFunctions,
   * it is used for transformations that can fail due to an
   * AnalysisException.
   */
  def transformationWithAnalysis(
      f: UnderlyingDataFrameStatFunctions => UnderlyingDataFrameStatFunctions
  ): TryAnalysis[DataFrameStatFunctions] = TryAnalysis(transformation(f))

  /** Applies an action to the underlying DataFrameStatFunctions. */
  def get[U](f: UnderlyingDataFrameStatFunctions => U): U = f(underlying)

  /**
   * Applies an action to the underlying DataFrameStatFunctions, it is
   * used for transformations that can fail due to an AnalysisException.
   */
  def getWithAnalysis[U](f: UnderlyingDataFrameStatFunctions => U): TryAnalysis[U] = TryAnalysis(f(underlying))

  // Generated functions coming from spark

  def bloomFilter(colName: String, expectedNumItems: Long, fpp: Double): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(colName, expectedNumItems, fpp))

  def bloomFilter(col: Column, expectedNumItems: Long, fpp: Double): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(col, expectedNumItems, fpp))

  def bloomFilter(colName: String, expectedNumItems: Long, numBits: Long): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(colName, expectedNumItems, numBits))

  def bloomFilter(col: Column, expectedNumItems: Long, numBits: Long): TryAnalysis[BloomFilter] =
    getWithAnalysis(_.bloomFilter(col, expectedNumItems, numBits))

  def corr(col1: String, col2: String, method: String): TryAnalysis[Double] =
    getWithAnalysis(_.corr(col1, col2, method))

  def corr(col1: String, col2: String): TryAnalysis[Double] = getWithAnalysis(_.corr(col1, col2))

  def countMinSketch(colName: String, depth: Int, width: Int, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(colName, depth, width, seed))

  def countMinSketch(colName: String, eps: Double, confidence: Double, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(colName, eps, confidence, seed))

  def countMinSketch(col: Column, depth: Int, width: Int, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(col, depth, width, seed))

  def countMinSketch(col: Column, eps: Double, confidence: Double, seed: Int): TryAnalysis[CountMinSketch] =
    getWithAnalysis(_.countMinSketch(col, eps, confidence, seed))

  def cov(col1: String, col2: String): TryAnalysis[Double] = getWithAnalysis(_.cov(col1, col2))

  // ===============

  def crosstab(col1: String, col2: String): TryAnalysis[DataFrame] = unpackWithAnalysis(_.crosstab(col1, col2))

  def freqItems(cols: Seq[String], support: Double): TryAnalysis[DataFrame] =
    unpackWithAnalysis(_.freqItems(cols, support))

  def freqItems(cols: Seq[String]): TryAnalysis[DataFrame] = unpackWithAnalysis(_.freqItems(cols))

  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): TryAnalysis[DataFrame] =
    unpackWithAnalysis(_.sampleBy[T](col, fractions, seed))

  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): TryAnalysis[DataFrame] =
    unpackWithAnalysis(_.sampleBy[T](col, fractions, seed))
}
