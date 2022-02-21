package zio.spark.internal.codegen

import zio.spark.internal.codegen.RDDAnalysis.MethodType._

import scala.reflect.runtime.universe

object RDDAnalysis {
  import scala.reflect.runtime.universe._

  def readMethodsApacheSparkRDD: Seq[universe.MethodSymbol] = {
    val tt = typeTag[org.apache.spark.rdd.RDD[Any]]
    tt.tpe.members.collect {
      case m: MethodSymbol if m.isMethod && m.isPublic => m
    }.toSeq
  }

  sealed trait MethodType
  object MethodType {

    case object Ignored                extends MethodType
    case object Transformation         extends MethodType
    case object SuccessNow             extends MethodType
    case object DriverAction           extends MethodType
    case object DistributedComputation extends MethodType
    case object ToImplement            extends MethodType
  }

  private def yolo() = {

    val rdd: org.apache.spark.rdd.RDD[Any] = ???

    rdd
  }

  private def getMethodType(method: universe.MethodSymbol): MethodType = {
    val cacheElements =
      Set(
        "getStorageLevel",
        "cache",
        "persist",
        "unpersist",
        "localCheckpoint",
        "checkpoint",
        "getResourceProfile",
        "getCheckpointFile",
        "isCheckpointed",
        "dependencies"
      )
    val partitionOps =
      Set("getNumPartitions", "partitions", "preferredLocations", "partitioner", "id", "countApproxDistinct")

    val otherTransformation = Set("barrier")
    val pureInfo            = Set("toDebugString")
    val action =
      Set(
        "isEmpty",
        "min",
        "max",
        "top",
        "first",
        "treeAggregate",
        "aggregate",
        "fold",
        "toLocalIterator",
        "treeReduce",
        "reduce",
        "collect"
      )

    val methodName = method.name.toString
    val returnType = method.returnType.typeSymbol

    println(methodName, returnType, method)
    method.name.toString match {
      case name if action(name)                                                     => DistributedComputation
      case name if name.startsWith("take")                                          => DistributedComputation
      case name if name.startsWith("foreach")                                       => DistributedComputation
      case name if name.startsWith("count")                                         => DistributedComputation
      case name if name.startsWith("saveAs")                                        => DistributedComputation
      case "iterator"                                                               => DistributedComputation
      case name if cacheElements(name)                                              => DriverAction
      case name if otherTransformation(name)                                        => SuccessNow
      case name if pureInfo(name)                                                   => SuccessNow
      case "sparkContext" | "context"                                               => ToImplement
      case "randomSplit"                                                            => ToImplement
      case _ if method.annotations.exists(_.toString.contains("DeveloperApi"))      => Ignored
      case "toJavaRDD"                                                              => Ignored
      case x if x.contains("$default$")                                             => Ignored
      case _ if method.fullName.startsWith("java.lang.Object.")                     => Ignored
      case _ if method.fullName.startsWith("scala.Any.")                            => Ignored
      case "$init$" | "toString"                                                    => Ignored
      case _ if method.isSetter                                                     => Ignored
      case "name"                                                                   => Ignored
      case name if partitionOps(name)                                               => Transformation
      case _ if method.returnType.typeSymbol.fullName == "org.apache.spark.rdd.RDD" => Transformation
    }

  }

  def main(args: Array[String]): Unit =
    println(
      readMethodsApacheSparkRDD
        .flatMap(method => scala.util.Try(getMethodType(method)).fold(x => Some(method.fullName), _ => None))
        .mkString("\n")
    )
}
