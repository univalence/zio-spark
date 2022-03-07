package zio.spark

import scala.reflect.ClassTag

package object rdd {
  implicit class PairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def reduceByKey(func: (V, V) => V): RDD[(K, V)] = rdd.transformation(_.reduceByKey(func))
  }

  implicit class RDDConversionOps[T](private val underlyingRDD: org.apache.spark.rdd.RDD[T]) extends AnyVal {
    @inline def zioSpark: RDD[T] = RDD(underlyingRDD)
  }
}
