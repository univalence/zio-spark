package zio.spark.internal.codegen

import zio.*
import zio.spark.internal.codegen.GenerationPlan.{
  DataFrameStatFunctionsPlan,
  DatasetPlan,
  RDDPlan,
  RelationalGroupedDatasetPlan
}
import zio.spark.internal.codegen.Helpers.{findMethod, planLayer}
import zio.spark.internal.codegen.MethodType.*
import zio.test.*

object MethodTypeSpec extends DefaultRunnableSpec {
  def testMethodTypeFor(name: String, arity: Int = -1, args: List[String] = Nil)(
      expected: MethodType
  ): ZSpec[GenerationPlan, Nothing] = {
    val outputName = if (args.isEmpty) name else s"$name(${args.mkString(", ")})"

    test(s"The function '$outputName' should be a $expected") {
      val maybeMethodEffect =
        for {
          generationPlan <- ZIO.service[GenerationPlan]
          method         <- ZIO.fromOption(findMethod(name, generationPlan, arity, args))
          methodType = getMethodType(method, generationPlan.planType)
        } yield methodType

      val res: URIO[GenerationPlan, TestResult] =
        maybeMethodEffect.fold(
          failure = _ => assertNever(s"can't find '$outputName'.'"),
          success = methodType => assertTrue(methodType == expected)
        )

      res
    }
  }

  val rddMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for RDD")(
      testMethodTypeFor("withResources")(Transformation),
      testMethodTypeFor("countApproxDistinct")(DistributedComputation)
    ).provideLayer(planLayer(RDDPlan))

  val datasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for Dataset")(
      testMethodTypeFor("as", arity = 1, args = List("alias"))(Transformation),
      testMethodTypeFor("as", arity = 0)(TransformationWithAnalysis),
      testMethodTypeFor("withColumn")(TransformationWithAnalysis),
      testMethodTypeFor("drop")(Transformation),
      testMethodTypeFor("persist")(DriverAction)
    ).provideLayer(planLayer(DatasetPlan))

  val relationalGroupedDatasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for RelationalGroupedDataset")(
      testMethodTypeFor("as")(GetWithAnalysis),
      testMethodTypeFor("count")(Unpack)
    ).provideLayer(planLayer(RelationalGroupedDatasetPlan))

  val dataFrameStatFunctionsMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for DataFrameStatFunctions")(
      testMethodTypeFor("countMinSketch")(GetWithAnalysis)
    ).provideLayer(planLayer(DataFrameStatFunctionsPlan))

  override def spec: ZSpec[TestEnvironment, Any] =
    rddMethodTypes + datasetMethodTypes + relationalGroupedDatasetMethodTypes + dataFrameStatFunctionsMethodTypes
}
