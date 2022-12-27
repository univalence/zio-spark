package zio.spark

import zio.Trace
import zio.spark.parameter._
import zio.spark.sql.{Dataset => DS, _}
import zio.spark.sql.implicits._

package object test {
  val defaultSparkSession: SparkSession.Builder =
    SparkSession.builder
      .master(localAllNodes)
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.ui.enabled", value = false)

  def Dataset[T: Encoder](values: T*)(implicit trace: Trace): SIO[DS[T]] = values.toDataset
}
