package zio.spark.internal.codegen

import zio.ZIO
import zio.spark.internal.codegen.GenerationPlan.{DatasetPlan, KeyValueGroupedDatasetPlan, RDDPlan}
import zio.spark.internal.codegen.Helpers.{findMethod, planLayer}
import zio.test.*

object MethodSpec extends DefaultRunnableSpec {
  def genTest2(name: String, arity: Int = -1, args: List[String] = Nil)(
      expectedCode: String
  ): ZSpec[GenerationPlan, Nothing] =
    test(name) {
      ZIO.serviceWith[GenerationPlan][TestResult] { plan =>
        findMethod(name, plan, arity, args) match {
          case Some(m) =>
            val generatedCode = m.toCode(MethodType.getMethodType(m, plan.planType))
            assertTrue(generatedCode.contains(expectedCode))
          case None => assertNever(s"can't find $name")
        }
      }
    }

  val rddMethods: Spec[Any, TestFailure[Nothing], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        expectedCode: String
    ): ZSpec[GenerationPlan, Nothing] = genTest2(methodName, arity, args)(expectedCode)

    suite("Check method generations for RDD")(
      checkGen("min")("min(implicit ord: Ordering[T], trace: ZTraceElement): Task[T]"),
      checkGen("collect", 0)("collect(implicit trace: ZTraceElement): Task[Seq[T]]"),
      checkGen("saveAsObjectFile")("saveAsObjectFile(path: => String)(implicit trace: ZTraceElement): Task[Unit]"),
      checkGen("countByValue")("Task[Map[T, Long]]"),
      checkGen("map")("map[U: ClassTag](f: T => U): RDD[U]"),
      checkGen("cache")("cache(implicit trace: ZTraceElement): Task[RDD[T]]"),
      checkGen("dependencies")("dependencies(implicit trace: ZTraceElement): Task[Seq[Dependency[_]]]"),
      checkGen("zipWithIndex")("zipWithIndex: RDD[(T, Long)]"),
      checkGen("countByValueApprox")("Task[PartialResult[Map[T, BoundedDouble]]]"),
      checkGen("distinct", 2)("distinct(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T]"),
      checkGen("saveAsTextFile", 2)(
        "saveAsTextFile(path: => String, codec: => Class[_ <: CompressionCodec])(implicit trace: ZTraceElement): Task[Unit]"
      )
    )
  }.provide(planLayer(RDDPlan))

  val datasetMethods: Spec[Any, TestFailure[Nothing], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        expectedCode: String
    ): ZSpec[GenerationPlan, Nothing] = genTest2(methodName, arity, args)(expectedCode)

    suite("Check method generations for Dataset")(
      checkGen("filter", 1, List("conditionExpr"))("filter(conditionExpr: String): TryAnalysis[Dataset[T]]"),
      checkGen("orderBy", arity = 1)("_.orderBy(sortExprs: _*)"),
      checkGen("explode", arity = 2)("explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A])"),
      checkGen("dropDuplicates", arity = 1)("dropDuplicates(colNames: Seq[String]): TryAnalysis[Dataset[T]]")
    )
  }.provide(planLayer(DatasetPlan))

  val keyValueGroupedDatasetMethods: Spec[Any, TestFailure[Nothing], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        genCodeFragment: String
    ): ZSpec[GenerationPlan, Nothing] = genTest2(methodName, arity, args)(genCodeFragment)

    suite("Check method generations for Dataset")(
      checkGen("cogroup")("other.underlying")
    )
  }.provide(planLayer(KeyValueGroupedDatasetPlan))

  override def spec: ZSpec[TestEnvironment, Any] = rddMethods + datasetMethods + keyValueGroupedDatasetMethods

}
