package zio.spark.internal.codegen.generation.template

import zio.spark.internal.codegen.ScalaBinaryVersion
import zio.spark.internal.codegen.generation.Environment
import zio.spark.internal.codegen.generation.template.Helper.*

case object RDDTemplate extends Template.Default {
  override def name: String = "RDD"

  override def typeParameters: List[String] = List("T")

  override def imports(environment: Environment): Option[String] =
    Some {
      val baseImports: String =
        """import org.apache.hadoop.io.compress.CompressionCodec
          |import org.apache.spark.{Dependency, Partition, Partitioner, TaskContext}
          |import org.apache.spark.partial.{BoundedDouble, PartialResult}
          |import org.apache.spark.rdd.{PartitionCoalescer, RDD => UnderlyingRDD, RDDBarrier}
          |import org.apache.spark.storage.StorageLevel
          |
          |import zio._
          |
          |import scala.collection.Map
          |import scala.io.Codec
          |import scala.reflect._
          |""".stripMargin

      environment.scalaVersion match {
        case ScalaBinaryVersion.V2_11 => baseImports
        case _ => s"""$baseImports
                     |import org.apache.spark.resource.ResourceProfile""".stripMargin
      }
    }

  override def implicits(environment: Environment): Option[String] =
    Some {
      s"""private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)
         |private implicit def arrayToSeq2[U](x: Underlying$name[Array[U]]): Underlying$name[Seq[U]] = x.map(_.toIndexedSeq)
         |@inline private def noOrdering[U]: Ordering[U] = null""".stripMargin
    }

  override def annotations(environment: Environment): Option[String] =
    Some("@SuppressWarnings(Array(\"scalafix:DisableSyntax.defaultArgs\", \"scalafix:DisableSyntax.null\"))")

  override def helpers: Helper = action && transformation && get
}
