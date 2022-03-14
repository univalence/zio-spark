package org.apache.spark

import org.apache.spark.scheduler.SparkListenerInterface

object SparkContextCompatibility {
  final def removeSparkListener(sc: SparkContext, listener: SparkListenerInterface): Unit =
    sc.listenerBus.removeListener(listener)
}
