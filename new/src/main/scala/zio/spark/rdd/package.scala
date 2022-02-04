package zio.spark

import org.apache.spark.rdd.{PairRDDFunctions => UnderlyingPairRDDFunctions}

import scala.reflect.ClassTag

package object rdd {
  implicit class PairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def reduceByKey(func: (V, V) => V): RDD[(K, V)] =
      rdd.transformation(new UnderlyingPairRDDFunctions(_).reduceByKey(func))
  }
}
