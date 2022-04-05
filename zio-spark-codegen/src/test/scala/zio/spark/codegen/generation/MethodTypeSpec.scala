package zio.spark.codegen.generation

import zio.{URIO, ZIO}
import zio.spark.codegen.Helpers.{findMethodDefault, planLayer}
import zio.spark.codegen.generation.MethodType.*
import zio.spark.codegen.generation.plan.Plan.*
import zio.spark.codegen.generation.plan.SparkPlan
import zio.test.{assertNever, assertTrue, Spec, TestFailure, TestResult, TestSuccess, ZIOSpecDefault, ZSpec}

object MethodTypeSpec extends ZIOSpecDefault {
  def testMethodTypeFor(name: String, arity: Int = -1, args: List[String] = Nil)(
      expected: MethodType
  ): ZSpec[SparkPlan, Nothing] = {
    val outputName = if (args.isEmpty) name else s"$name(${args.mkString(", ")})"

    test(s"The function '$outputName' should be a $expected") {
      val maybeMethodEffect =
        for {
          plan        <- ZIO.service[SparkPlan]
          maybeMethod <- findMethodDefault(name, arity, args)
          method      <- ZIO.fromOption(maybeMethod)
          methodType = plan.template.getMethodType(method)
        } yield methodType

      val res: URIO[SparkPlan, TestResult] =
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
    ).provide(planLayer(rddPlan))

  val datasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for Dataset")(
      testMethodTypeFor("as", arity = 1, args = List("alias"))(Transformation),
      testMethodTypeFor("as", arity = 0)(TransformationWithAnalysis),
      testMethodTypeFor("withColumn")(TransformationWithAnalysis),
      testMethodTypeFor("drop")(Transformation),
      testMethodTypeFor("persist")(DriverAction)
    ).provide(planLayer(datasetPlan))

  val relationalGroupedDatasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for RelationalGroupedDataset")(
      testMethodTypeFor("as")(GetWithAnalysis),
      testMethodTypeFor("count")(Unpack)
    ).provide(planLayer(relationalGroupedDatasetPlan))

  val dataFrameStatFunctionsMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for DataFrameStatFunctions")(
      testMethodTypeFor("countMinSketch")(GetWithAnalysis)
    ).provide(planLayer(dataFrameStatFunctionsPlan))

  val dataFrameNaFunctionsMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for DataFrameNaFunctions")(
      testMethodTypeFor("drop", arity = 1, args = List("cols"))(UnpackWithAnalysis)
    ).provide(planLayer(dataFrameNaFunctionsPlan))

  val keyValueGroupedDatasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for KeyValueGroupedDataset")(
      testMethodTypeFor("count")(Unpack),
      testMethodTypeFor("keyAs")(Transformation),
      testMethodTypeFor("mapValues")(Transformation)
    ).provide(planLayer(keyValueGroupedDatasetPlan))

  override def spec: ZSpec[Any, Any] = {
    val specs =
      Seq(
        rddMethodTypes,
        datasetMethodTypes,
        relationalGroupedDatasetMethodTypes,
        dataFrameStatFunctionsMethodTypes,
        dataFrameNaFunctionsMethodTypes,
        keyValueGroupedDatasetMethodTypes
      )

    specs.reduce(_ + _)
  }
}
