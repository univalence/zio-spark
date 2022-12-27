package zio.spark.sql

import scala3encoders.given
import org.apache.spark.sql.{ColumnName, Encoder}
import zio.spark.rdd.{RDD, RDDConversionOps}
import zio.*

import scala.reflect.ClassTag

object implicits {
  extension (sc: StringContext) {
    def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
  }

  extension [T: Encoder](seq: Seq[T]) {
    def toDataset(implicit trace: Trace): SIO[Dataset[T]] =
      zio.spark.sql.fromSpark(ss => ss.implicits.localSeqToDatasetHolder(seq).toDS().zioSpark).orDie

    def toDS(implicit trace: Trace):  SIO[Dataset[T]] = toDataset
  }

  extension [T: ClassTag](seq: Seq[T]) {
    def toRDD(implicit trace: Trace): URIO[SparkSession, RDD[T]] =
      zio.spark.sql.fromSpark(_.sparkContext.makeRDD(seq).zioSpark).orDie
  }

}