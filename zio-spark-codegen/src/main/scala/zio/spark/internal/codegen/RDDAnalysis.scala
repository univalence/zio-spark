package zio.spark.internal.codegen

import zio.spark.internal.codegen.RDDAnalysis.MethodType._

import scala.reflect.runtime.universe

object RDDAnalysis {

  // TODO : IGNORE
  // def foreachPartition(func: ForeachPartitionFunction[T]): Task[Unit] = action(_.foreachPartition(func))

  val listOfMethodsWithImplicitNullOrdering =
    Seq(
      "distinct",
      "repartition",
      "coalesce",
      "intersection",
      "groupBy",
      "groupBy",
      "subtract",
      "countByValue",
      "countByValueApprox"
    )
  // rddToPairRDDFunctions

  /**
   * TODO : Manage default values
   *
   * Default values are managed using alternate methods in the class
   * definition They are not available in the MethodSymbol
   *
   * There are 3 strategies to manage default values
   *   1. dirty : listOfMethodsWithImplicitNullOrdering, list the
   *      methods that have default arguments, like implicit
   *      ord:Ordering[T] = null, propagate the information downstream
   *      to generate correctly
   *
   * 2. mapping hack :
   * RDDAnalysis.readMethodsApacheSparkRDD.count(_.fullName.contains("$default$"))
   * isolate the default methods, build a Map MethodName -> (Map ArgName
   * -> Type) if the method m1 generated can overlap a default method
   * with the same name (all args of the default method are in m1) set
   * the missing argument to default value (null for Ordering, None for
   * Option, Random for Long ? (seed)
   *
   * 3. scala meta from the future get the information from the source
   * directly, using ScalaMeta to parse the org.apache.spark.rdd.RDD
   * source file generate from this information
   */

  import scala.reflect.runtime.universe.*

  def readMethodsApacheSparkRDD: Seq[universe.MethodSymbol] = {
    val tt = typeTag[org.apache.spark.rdd.RDD[Any]]
    tt.tpe.members.collect {
      case m: MethodSymbol if m.isMethod && m.isPublic => m
    }.toSeq
  }

  implicit val orderingMethodType: Ordering[MethodType] =
    (x: MethodType, y: MethodType) => {
      def methodTypeToInt(methodType: MethodType): Int =
        methodType match {
          case MethodType.SuccessNow             => 0
          case MethodType.DistributedComputation => 1
          case MethodType.DriverAction           => 2
          case MethodType.Transformation         => 3
          case MethodType.ToImplement            => 4
          case MethodType.Ignored                => 5
        }

      Ordering[Int].compare(methodTypeToInt(x), methodTypeToInt(y))
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

  def getMethodType(method: Method, path: String): MethodType = {
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

    def checkForJavaArgs: Boolean =
      method.calls.exists(_.symbols.exists(_.typeSignature.toString.contains("org.apache.spark.api.java.function")))

    method.name match {
      case "takeAsList"                                               => Ignored // return java.util.List
      case "saveAsTextFile"                                           => ToImplement
      case x if x.contains("$")                                       => Ignored
      case _ if method.annotations.exists(_.contains("DeveloperApi")) => Ignored
      case _ if checkForJavaArgs                                      => Ignored
      case "transform"                        => ToImplement // codegen hard to do for an helper method
      case "explode"                          => ToImplement // codegen not perfect due to contextBound on A
      case name if action(name)               => DistributedComputation
      case name if name.startsWith("take")    => DistributedComputation
      case name if name.startsWith("foreach") => DistributedComputation
      case name if name.startsWith("count")   => DistributedComputation
      case name if name.startsWith("saveAs")  => DistributedComputation
      case "iterator"                         => DistributedComputation
      case name if cacheElements(name)        => DriverAction
      case name if otherTransformation(name)  => SuccessNow
      case name if pureInfo(name)             => SuccessNow
      case name if partitionOps(name)         => SuccessNow
      case "sparkContext" | "context"         => ToImplement
      case "randomSplit"                      => ToImplement
      case "toJavaRDD"                        => ToImplement
      case _ if method.path.startsWith("java.lang.Object") => Ignored
      case _ if method.path.startsWith("scala.Any")        => Ignored
      case "toString"                                      => Ignored
      case _ if method.isSetter                            => Ignored
      case "name"                                          => DriverAction
      case _ if method.returnType.fullName == path         => Transformation
      case _                                               => Ignored // TODO: remove this one when dataset are handled
    }
  }
}
