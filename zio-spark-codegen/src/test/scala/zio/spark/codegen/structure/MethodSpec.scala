package zio.spark.codegen.structure

import zio.{URIO, ZIO}
import zio.spark.codegen.Helpers.{findMethodDefault, planLayer}
import zio.spark.codegen.generation.plan.Plan.{datasetPlan, keyValueGroupedDatasetPlan, rddPlan}
import zio.spark.codegen.generation.plan.SparkPlan
import zio.test.{assertNever, assertTrue, Spec, TestResult, ZIOSpecDefault}

object MethodSpec extends ZIOSpecDefault {
  def genTest2(name: String, arity: Int = -1, args: List[String] = Nil)(
      expectedCode: String
  ): Spec[SparkPlan, Nothing] =
    test(name) {
      val res: URIO[SparkPlan, TestResult] =
        for {
          plan        <- ZIO.service[SparkPlan]
          maybeMethod <- findMethodDefault(name, arity, args).orDieWith(e => new Throwable(e.msg))
        } yield maybeMethod match {
          case Some(m) =>
            val generatedCode = m.toCode(plan.template.getMethodType(m), plan)
            assertTrue(generatedCode.contains(expectedCode))
          case None => assertNever(s"can't find $name")
        }

      res
    }

  val rddMethods: Spec[Any, Nothing] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        expectedCode: String
    ): Spec[SparkPlan, Nothing] = genTest2(methodName, arity, args)(expectedCode)

    suite("Check method generations for RDD")(
      checkGen("min")("min(implicit ord: Ordering[T], trace: Trace): Task[T]"),
      checkGen("collect", 0)("collect(implicit trace: Trace): Task[Seq[T]]"),
      checkGen("saveAsObjectFile")("saveAsObjectFile(path: => String)(implicit trace: Trace): Task[Unit]"),
      checkGen("countByValue")("Task[Map[T, Long]]"),
      checkGen("map")("map[U: ClassTag](f: T => U): RDD[U]"),
      checkGen("cache")("cache(implicit trace: Trace): Task[RDD[T]]"),
      checkGen("dependencies")("dependencies(implicit trace: Trace): Task[Seq[Dependency[_]]]"),
      checkGen("zipWithIndex")("zipWithIndex: RDD[(T, Long)]"),
      checkGen("countByValueApprox")("Task[PartialResult[Map[T, BoundedDouble]]]"),
      checkGen("distinct", 2)("distinct(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T]"),
      checkGen("saveAsTextFile", 2)(
        "saveAsTextFile(path: => String, codec: => Class[_ <: CompressionCodec])(implicit trace: Trace): Task[Unit]"
      )
    )
  }.provide(planLayer(rddPlan))

  val datasetMethods: Spec[Any, Nothing] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        expectedCode: String
    ): Spec[SparkPlan, Nothing] = genTest2(methodName, arity, args)(expectedCode)

    suite("Check method generations for Dataset")(
      checkGen("filter", 1, List("conditionExpr"))("filter(conditionExpr: String): TryAnalysis[Dataset[T]]"),
      checkGen("orderBy", arity = 1)("_.orderBy(sortExprs: _*)"),
      checkGen("explode", arity = 2)("explode[A <: Product : TypeTag](input: Column*)(f: Row => IterableOnce[A])"),
      checkGen("dropDuplicates", arity = 1)("dropDuplicates(colNames: Seq[String]): TryAnalysis[Dataset[T]]")
    )
  }.provide(planLayer(datasetPlan))

  val keyValueGroupedDatasetMethods: Spec[Any, Nothing] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        genCodeFragment: String
    ): Spec[SparkPlan, Nothing] = genTest2(methodName, arity, args)(genCodeFragment)

    suite("Check method generations for Dataset")(
      checkGen("cogroup")("other.underlying")
    )
  }.provide(planLayer(keyValueGroupedDatasetPlan))

  override def spec: Spec[Any, Any] = rddMethods + datasetMethods + keyValueGroupedDatasetMethods

}
