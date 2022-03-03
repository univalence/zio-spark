package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}
import zio.spark.internal.Impure
import zio.spark.internal.Impure.ImpureBox

abstract class ExtraSparkSessionFeature(underlyingSparkSession: ImpureBox[UnderlyingSparkSession])
  extends Impure(underlyingSparkSession)