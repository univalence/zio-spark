package zio.spark.rdd

import org.apache.spark.rdd.{RDD => UnderlyingRDD}

import zio.spark.impure.Impure.ImpureBox
import zio.spark.internal.codegen.BaseRDD

final case class RDD[T](underlyingRDD: ImpureBox[UnderlyingRDD[T]]) extends BaseRDD(underlyingRDD)
