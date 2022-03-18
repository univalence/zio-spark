package zio.spark.internal.codegen

import zio.*
import zio.spark.internal.codegen.GenerationPlan.{DatasetPlan, RDDPlan, RelationalGroupedDatasetPlan}
import zio.spark.internal.codegen.Helpers.{findMethod, planLayer}
import zio.spark.internal.codegen.MethodType.*
import zio.test.*

object MethodTypeSpec extends DefaultRunnableSpec {
  def testMethodTypeFor(name: String)(expected: MethodType): ZSpec[GenerationPlan, Nothing] =
    test(s"The function $name should be a $expected") {
      val maybeMethodEffect =
        for {
          generationPlan <- ZIO.service[GenerationPlan]
          method         <- ZIO.fromOption(findMethod(name, generationPlan, -1, Nil))
          methodType = getMethodType(method, generationPlan.planType)
        } yield methodType

      val res: URIO[GenerationPlan, TestResult] =
        maybeMethodEffect.fold(
          failure = _ => assertNever(s"can't find $name"),
          success = methodType => assertTrue(methodType == expected)
        )

      res
    }

  val rddMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for RDD")(
      testMethodTypeFor("withResources")(Transformation),
      testMethodTypeFor("countApproxDistinct")(DistributedComputation)
    ).provideLayer(planLayer(RDDPlan))

  val datasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for RDD")(
      testMethodTypeFor("withColumn")(TransformationWithAnalysis),
      testMethodTypeFor("drop")(Transformation)
    ).provideLayer(planLayer(DatasetPlan))

  val relationalGroupedDatasetMethodTypes: Spec[Any, TestFailure[Nothing], TestSuccess] =
    suite("Check method types for RDD")(
      testMethodTypeFor("as")(GetWithAnalysis),
      testMethodTypeFor("count")(Unpack)
    ).provideLayer(planLayer(RelationalGroupedDatasetPlan))

  override def spec: ZSpec[TestEnvironment, Any] = rddMethodTypes + datasetMethodTypes + relationalGroupedDatasetMethodTypes
}
