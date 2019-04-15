package io.univalence.sparkzio

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.FunSuite
import scalaz.zio.{ DefaultRuntime, Task }

class SparkEnvImplicitClassTest extends FunSuite {
  val runtime: DefaultRuntime = new DefaultRuntime {}
  val ss: SparkSession        = SparkSession.builder.master("local[*]").getOrCreate()
  val sparkEnv: SparkZIO      = new SparkZIO(ss)

}
