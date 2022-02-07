package zio.spark.rdd

import zio.spark.CompatibilityTestBetween

object RDDCompatibilityTest
    extends CompatibilityTestBetween[org.apache.spark.rdd.RDD[Any], zio.spark.rdd.RDD[Any]](
      allowedNewMethods = Seq("underlyingRDD", "transformation", "action"),
      isImpure          = true
    )
