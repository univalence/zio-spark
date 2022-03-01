package zio.spark.internal.codegen

import zio.spark.internal.codegen.structure.Method

sealed trait MethodType

object MethodType {
  case object Ignored                extends MethodType
  case object Transformation         extends MethodType
  case object SuccessNow             extends MethodType
  case object DriverAction           extends MethodType
  case object DistributedComputation extends MethodType
  case object TODO                   extends MethodType
  case object ToImplement            extends MethodType

  implicit val orderingMethodType: Ordering[MethodType] =
    (x: MethodType, y: MethodType) => {
      def methodTypeToInt(methodType: MethodType): Int =
        methodType match {
          case MethodType.SuccessNow             => 0
          case MethodType.DistributedComputation => 1
          case MethodType.DriverAction           => 2
          case MethodType.Transformation         => 3
          case MethodType.TODO                   => 4
          case MethodType.ToImplement            => 5
          case MethodType.Ignored                => 6
        }

      Ordering[Int].compare(methodTypeToInt(x), methodTypeToInt(y))
    }

  def getMethodType(method: Method): MethodType = {
    val cacheElements =
      Set(
        "getStorageLevel",
        "storageLevel",
        "cache",
        "persist",
        "unpersist",
        "localCheckpoint",
        "checkpoint",
        "getResourceProfile",
        "getCheckpointFile",
        "isCheckpointed",
        "dependencies",
        "registerTempTable",
        "createTempView",
        "createOrReplaceTempView",
        "createGlobalTempView",
        "createOrReplaceGlobalTempView"
      )

    val getters =
      Set(
        "name",
        "schema",
        "dtypes",
        "columns",
        "isLocal",
        "isStreaming",
        "inputFiles"
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
        "collect",
        "tail",
        "head",
        "collect",
        "isEmpty"
      )

    val methodsToImplement =
      Set(
        "show",      // It should be implemented using Console layer,
        "transform", // Too specific for codegen
        "context",   // TODO: explain why
        "write"      // TODO: DataFrameWriter should be added to zio-spark
      )

    val methodsTodo =
      Set(
        "context",      // TODO: explain why
        "sparkContext", // TODO: explain why
        "randomSplit",  // It should be implemented using Random layer
        "printSchema",  // It should be implemented using Console layer
        "explain",      // It should be implemented using Console layer
        "na",           // TODO: DataFrameNaFunctions should be added to zio-spark
        "stat",         // TODO: DataFrameStatFunctions should be added to zio-spark
        "groupBy",      // TODO: RelationalGroupedDataset should be added to zio-spark
        "rollup",       // TODO: RelationalGroupedDataset should be added to zio-spark
        "cube",         // TODO: RelationalGroupedDataset should be added to zio-spark
        "groupByKey",   // TODO: KeyValueGroupedDataset should be added to zio-spark
        "writeTo",      // TODO: DataFrameWriterV2 should be added to zio-spark
        "writeStream"   // TODO: DataStreamWriter should be added to zio-spark
      )
    val methodsToIgnore =
      Set(
        "takeAsList",        // Java specific implementation
        "toJavaRDD",         // Java specific implementation
        "javaRDD",           // Java specific implementation
        "randomSplitAsList", // Java specific implementation
        "collectAsList",     // Java specific implementation
        "toString",          // TODO: explain why
        "apply",             // TODO: ignored temporarily
        "col",               // TODO: ignored temporarily
        "colRegex"           // TODO: ignored temporarily
      )

    // def checkForJavaArgs: Boolean =
    //  method.calls.exists(_.symbols.exists(_.typeSignature.toString.contains("org.apache.spark.api.java.function")))

    method.name match {
      case name if methodsToImplement(name) => ToImplement
      case name if methodsToIgnore(name)    => Ignored
      case name if methodsTodo(name)        => TODO
      case name if name.contains("$")       => Ignored
      // case _ if method.annotations.exists(_.contains("DeveloperApi")) => Ignored
      // case _ if checkForJavaArgs                                      => Ignored
      case _ if method.calls.flatMap(_.parameters.map(_.signature)).exists(_.contains("Function")) => Ignored
      case name if action(name)                            => DistributedComputation
      case name if name.startsWith("take")                 => DistributedComputation
      case name if name.startsWith("foreach")              => DistributedComputation
      case name if name.startsWith("count")                => DistributedComputation
      case name if name.startsWith("saveAs")               => DistributedComputation
      case "iterator"                                      => DistributedComputation
      case name if cacheElements(name)                     => DriverAction
      case name if getters(name)                           => DriverAction
      case name if otherTransformation(name)               => SuccessNow
      case name if pureInfo(name)                          => SuccessNow
      case name if partitionOps(name)                      => SuccessNow
      case _ if method.path.startsWith("java.lang.Object") => Ignored
      case _ if method.path.startsWith("scala.Any")        => Ignored
      case _ if method.isSetter                            => Ignored
      case _ if method.returnType.startsWith("RDD")        => Transformation
      case _ if method.returnType.startsWith("Dataset")    => Transformation
      case _ if method.returnType == "DataFrame"           => Transformation
      case _ if method.returnType.contains("this.type")    => Transformation
    }
  }
}
