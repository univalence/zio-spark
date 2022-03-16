package zio.spark.internal.codegen

import zio.spark.internal.codegen.GenerationPlan.{DataFrameNaFunctionsPlan, PlanType}
import zio.spark.internal.codegen.structure.Method

sealed trait MethodType

object MethodType {
  case object Ignored                    extends MethodType
  case object Transformation             extends MethodType
  case object TransformationWithAnalysis extends MethodType
  case object GetWithAnalysis            extends MethodType
  case object Get                        extends MethodType
  case object Unpack                     extends MethodType
  case object UnpackWithAnalysis         extends MethodType
  case object DriverAction               extends MethodType
  case object DistributedComputation     extends MethodType
  case object TODO                       extends MethodType
  case object ToImplement                extends MethodType

  def methodTypeOrdering(methodType: MethodType): Int =
    methodType match {
      case MethodType.Get                        => 0
      case MethodType.GetWithAnalysis            => 1
      case MethodType.DistributedComputation     => 2
      case MethodType.DriverAction               => 3
      case MethodType.Transformation             => 4
      case MethodType.TransformationWithAnalysis => 5
      case MethodType.Unpack                     => 6
      case MethodType.UnpackWithAnalysis         => 7
      case MethodType.TODO                       => 8
      case MethodType.ToImplement                => 9
      case MethodType.Ignored                    => 10
    }

  implicit val orderingMethodType: Ordering[MethodType] = Ordering.by(methodTypeOrdering)

  def getMethodType(method: Method, planType: PlanType): MethodType = {
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
        "barrier",
        "name",
        "dtypes",
        "isLocal",
        "isStreaming",
        "inputFiles"
      )

    val partitionOps =
      Set("getNumPartitions", "partitions", "preferredLocations", "partitioner", "id", "countApproxDistinct")

    val pureInfo = Set("schema", "columns", "toDebugString", "na", "stat")

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
        "collect"
      )

    val methodsToImplement =
      Set(
        "explain",     // It should be implemented using Console layer
        "show",        // It should be implemented using Console layer
        "printSchema", // It should be implemented using Console layer
        "transform",   // Too specific for codegen
        "write",       // TODO: DataFrameWriter should be added to zio-spark
        "groupBy"      // TODO: RelationalGroupedDataset should be added to zio-spark
      )

    val methodsTodo =
      Set(
        "context",      // TODO: SparkContext should be wrapped
        "sparkContext", // TODO: SparkContext should be wrapped
        "randomSplit",  // It should be implemented using Random layer
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
        "toString"           // TODO: explain why
      )

    val parameterProvokingAnalysis = Set("expr", "condition", "col", "valueMap")

    def oneOfContains(elements: Seq[String], candidates: Set[String]): Boolean =
      elements.exists(element => candidates.exists(candidate => element.contains(candidate)))

    val returnDataset        = method.returnType.startsWith("Dataset") || method.returnType == "DataFrame"
    val shouldUseTryAnalysis = oneOfContains(method.anyParameters.map(_.name.toLowerCase), parameterProvokingAnalysis)

    method.name match {
      case _ if method.fullName.startsWith("java.lang.Object")                                     => Ignored
      case _ if method.fullName.startsWith("scala.Any")                                            => Ignored
      case _ if method.isSetter                                                                    => Ignored
      case name if name == "groupBy" && method.fullName.contains("RDD")                            => Ignored
      case name if methodsToIgnore(name)                                                           => Ignored
      case name if name.contains("$")                                                              => Ignored
      case _ if method.calls.flatMap(_.parameters.map(_.signature)).exists(_.contains("Function")) => Ignored
      case name if methodsToImplement(name)                                                        => ToImplement
      case name if methodsTodo(name)                                                               => TODO
      case "drop" if planType.name == "Dataset"                                                    => Transformation
      case "count" if planType.name == "RelationalGroupedDataset"                                  => Unpack
      case "as" if planType.name == "RelationalGroupedDataset"                                     => GetWithAnalysis
      case "apply" | "col" | "colRegex" | "withColumn" if method.fullName.contains("Dataset")      => GetWithAnalysis
      case "bloomFilter" | "corr" | "countMinSketch" | "cov" if planType.name == "DataFrameStatFunctions" =>
        GetWithAnalysis
      case _ if shouldUseTryAnalysis && returnDataset && planType.name != "Dataset" => UnpackWithAnalysis
      case _ if shouldUseTryAnalysis                                                => TransformationWithAnalysis
      case name if method.anyParameters.isEmpty && name == "as"                     => TransformationWithAnalysis
      case name if action(name)                                                     => DistributedComputation
      case name if name.startsWith("take")                                          => DistributedComputation
      case name if name.startsWith("foreach")                                       => DistributedComputation
      case name if name.startsWith("count")                                         => DistributedComputation
      case name if name.startsWith("saveAs")                                        => DistributedComputation
      case "iterator"                                                               => DistributedComputation
      case name if cacheElements(name)                                              => DriverAction
      case name if getters(name)                                                    => DriverAction
      case name if pureInfo(name)                                                   => Get
      case name if partitionOps(name)                                               => Get
      case _ if method.returnType.startsWith("RDD")                                 => Transformation
      case _ if method.returnType.contains("this.type")                             => Transformation
      case _ if returnDataset && planType.name != "Dataset"                         => Unpack
      case _ if returnDataset                                                       => Transformation
    }
  }
}
