package zio.spark.codegen.generation.template.instance

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.MethodType
import zio.spark.codegen.generation.template.{Helper, Template}
import zio.spark.codegen.generation.template.Helper.*
import zio.spark.codegen.structure.Method

case object SparkContextTemplate extends Template.Default {
  override def name: String = "SparkContext"

  override def imports(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      val baseImports: String =
        """
          |import zio._
          |import zio.spark.rdd.RDD
          |
          |import org.apache.hadoop.conf.Configuration
          |import org.apache.hadoop.mapred.{InputFormat, JobConf}
          |import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
          |import org.apache.spark.broadcast.Broadcast
          |import org.apache.spark.input.PortableDataStream
          |import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
          |import org.apache.spark.util._
          |import org.apache.spark.{SimpleFutureAction, SparkConf, SparkStatusTracker, TaskContext, SparkContext => UnderlyingSparkContext}
          |import org.apache.spark.rdd.{RDD => UnderlyingRDD}
          |
          |import scala.reflect.ClassTag
          |""".stripMargin

      scalaVersion match {
        case ScalaBinaryVersion.V2_11 =>
          s"""$baseImports
             |import org.apache.spark.{
             |  Accumulable,
             |  Accumulator,
             |  AccumulatorParam,
             |  AccumulableParam
             |}
             |import scala.collection.generic.Growable""".stripMargin
        case _ =>
          s"""$baseImports
             |import org.apache.spark.resource._""".stripMargin
      }
    }

  override def implicits(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      s"""private implicit def lift[U](x:UnderlyingRDD[U]):RDD[U] =
         |  RDD(x)
         |private implicit def liftMap[K, V](map: scala.collection.Map[K, V]): Map[K, V] =
         |  map.toMap""".stripMargin
    }

  override def helpers: Helper = action && get

  override def annotations(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some("@SuppressWarnings(Array(\"scalafix:DisableSyntax.defaultArgs\"))")

  private def isGetter(method: Method): Boolean = {
    val getters = Set(
      "startTime",
      "makeRDD",
      "range",
      "hadoopFile",
      "newAPIHadoopRDD",
      "emptyRDD",
      "union",
      "getConf",
      "jars",
      "files",
      "master",
      "deployMode",
      "appName",
      "isLocal",
      "isStopped",
      "statusTracker",
      "uiWebUrl",
      "hadoopConfiguration",
      "sparkUser",
      "applicationId",
      "applicationAttemptId",
      "defaultParallelism",
      "defaultMinPartitions"
    )

    getters(method.name)
  }

  override def getMethodType(method: Method): MethodType =
    method match {
      case _ if isGetter(method)                   => MethodType.Get
      case _ if method.name == "sequenceFile"      => MethodType.Ignored
      case _ if method.name == "getPersistentRDDs" => MethodType.ToImplement
      case _                                       => MethodType.DriverAction
    }
}
