package zio.spark.internal.codegen

import zio.spark.internal.codegen.GenerationPlan.*
import zio.spark.internal.codegen.structure.Method

sealed trait MethodType {
  def withAnalysis: MethodType =
    this match {
      case MethodType.Transformation => MethodType.TransformationWithAnalysis
      case MethodType.Get            => MethodType.GetWithAnalysis
      case MethodType.Unpack         => MethodType.UnpackWithAnalysis
      case m                         => m
    }
}

object MethodType {
  case object Ignored                    extends MethodType // Methods will not be available in zio-spark
  case object ToHandle                   extends MethodType // Methods are not handler by zio-spark for now
  case object ToImplement                extends MethodType // Methods need to be implemented manually in "it"
  case object Transformation             extends MethodType // T => T
  case object TransformationWithAnalysis extends MethodType // T => TryAnalysis[T]
  case object Get                        extends MethodType // T => U
  case object GetWithAnalysis            extends MethodType // T => TryAnalysis[U]
  case object Unpack                     extends MethodType // T => DataFrame
  case object UnpackWithAnalysis         extends MethodType // T => TryAnalysis[DataFrame]
  case object DriverAction               extends MethodType // T => Task[U]
  case object DistributedComputation     extends MethodType // T => Task[U]

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
      case MethodType.ToHandle                   => 8
      case MethodType.ToImplement                => 9
      case MethodType.Ignored                    => 10
    }

  implicit val orderingMethodType: Ordering[MethodType] = Ordering.by(methodTypeOrdering)

  def returnDataset(returnType: String): Boolean = returnType.matches("DataFrame|Dataset.*")

  def isTransformation(planType: PlanType, returnType: String): Boolean =
    planType match {
      case _ if returnType == "this.type"  => true
      case DatasetPlan                     => returnDataset(returnType)
      case RDDPlan                         => returnType.matches("RDD\\[.*\\]")
      case plan if plan.name == returnType => true
      case _                               => false
    }

  def isDriverAction(method: Method): Boolean = {
    val driverActions =
      Vector(
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
        "createOrReplaceGlobalTempView",
        "barrier",
        "name",
        "dtypes",
        "isLocal",
        "isStreaming",
        "inputFiles"
      )

    driverActions.exists(method.name.matches)
  }

  def isDistributedComputation(method: Method): Boolean = {
    val distributedComputations: Vector[String] =
      Vector(
        "countApproxDistinct",
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
        "iterator",
        "take.*",
        "foreach.*",
        "count.*",
        "saveAs.*"
      )

    distributedComputations.exists(method.name.matches)
  }

  def isIgnoredMethod(method: Method): Boolean = {
    val methodsToIgnore =
      Set(
        "takeAsList",        // Java specific implementation
        "toJavaRDD",         // Java specific implementation
        "javaRDD",           // Java specific implementation
        "randomSplitAsList", // Java specific implementation
        "collectAsList",     // Java specific implementation
        "toString"           // Wrapper are using case classes.toString is already implemented using underlying.toString.
      )

    method.name match {
      case _ if method.fullName.startsWith("java.lang.Object")                       => true
      case _ if method.fullName.startsWith("scala.Any")                              => true
      case _ if method.isSetter                                                      => true
      case name if name == "groupBy" && method.fullName.contains("RDD")              => true
      case name if methodsToIgnore(name)                                             => true
      case name if name.contains("$")                                                => true
      case _ if method.anyParameters.map(_.signature).exists(_.contains("Function")) => true
      case _                                                                         => false
    }
  }

  val methodsToImplement =
    Set(
      "explain",     // It should be implemented using Console layer
      "show",        // It should be implemented using Console layer
      "printSchema", // It should be implemented using Console layer
      "transform",   // Too specific for codegen
      "write",       // TODO: DataFrameWriter should be added to zio-spark
      "groupBy"      // TODO: RelationalGroupedDataset should be added to zio-spark
    )

  val methodsToHandle =
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

  val methodsGet =
    Set(
      "apply",
      "col",
      "colRegex",
      "getNumPartitions",
      "partitions",
      "preferredLocations",
      "partitioner",
      "id",
      "schema",
      "columns",
      "toDebugString",
      "na",
      "stat",
      "bloomFilter",
      "corr",
      "countMinSketch",
      "cov"
    )

  /**
   * Use a best effort first attempt to guess the method type of a
   * method according to its plan type. Because there is some edge
   * cases, we have to specify the type later on.
   */
  def getBaseMethodType(method: Method, planType: PlanType): MethodType = {
    val isATransformation = isTransformation(planType, method.returnType)

    method match {
      case m if isIgnoredMethod(m)          => Ignored
      case m if methodsToImplement(m.name)  => ToImplement
      case m if methodsToHandle(m.name)     => ToHandle
      case m if isDriverAction(m)           => DriverAction
      case _ if isATransformation           => Transformation
      case m if methodsGet(m.name)          => Get
      case m if isDistributedComputation(m) => DistributedComputation
      case m if returnDataset(m.returnType) => Unpack
      case _                                => ToHandle
    }
  }

  /** Get the method type of a given method and its plan type. */
  def getMethodType(method: Method, planType: PlanType): MethodType = {
    val baseMethodType = getBaseMethodType(method, planType)

    val datasetWithAnalysis       = Set("apply", "col", "colRegex", "withColumn")
    val dataframeStatWithAnalysis = Set("bloomFilter", "corr", "countMinSketch", "cov")
    val parameterProvokingAnalysis =
      Vector(
        "exprs?",
        "condition",
        "cols?",
        "col\\d",
        "valueMap",
        "(agg|partition|condition|sort|join)Exprs?",
        "colNames?",
        "(input|output|using|pivot)Columns?",
        "sortCols?",
        "allowMissingColumns" // for unionByName
      )

    val shouldUseTryAnalysis = method.anyParameters.exists(p => parameterProvokingAnalysis.exists(p.name.matches))

    (planType, method.name) match {
      case (RelationalGroupedDatasetPlan, "as")                                  => GetWithAnalysis
      case (RelationalGroupedDatasetPlan, "count")                               => Unpack
      case (RelationalGroupedDatasetPlan, "min" | "max")                         => UnpackWithAnalysis
      case (DatasetPlan, "drop")                                                 => baseMethodType
      case (DatasetPlan, name) if datasetWithAnalysis(name)                      => baseMethodType.withAnalysis
      case (DataFrameStatFunctionsPlan, name) if dataframeStatWithAnalysis(name) => baseMethodType.withAnalysis
      case _ if shouldUseTryAnalysis                                             => baseMethodType.withAnalysis
      case (_, "as") if method.anyParameters.isEmpty                             => baseMethodType.withAnalysis
      case _                                                                     => baseMethodType
    }
  }
}
