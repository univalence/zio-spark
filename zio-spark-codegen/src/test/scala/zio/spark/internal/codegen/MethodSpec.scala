package zio.spark.internal.codegen

import zio.spark.internal.codegen.structure.Method
import zio.test.*
import zio.test.TestAspect.*

object MethodSpec extends DefaultRunnableSpec {
  def genTest2(
      plan: GenerationPlan
  )(name: String, arity: Int = -1, args: List[String] = Nil)(generatedCode: String): ZSpec[Any, Nothing] = {
    def findMethod(name: String, arity: Int): Option[Method] = {
      val maybeMethod =
        plan.methods.find { method =>
          val allParams = method.calls.flatMap(_.parameters)
          method.name == name &&
          allParams.size == arity &&
          args.forall(allParams.map(_.name).contains(_))
        }
      maybeMethod orElse plan.methods.find(_.name == name)
    }

    val maybeMethod = findMethod(name, arity)

    test(name) {
      maybeMethod.fold(assertNever(s"can't find $name"))(m =>
        assertTrue(m.toCode(RDDAnalysis.getMethodType(m)).contains(generatedCode))
      )
    }
  }

  val rddMethods: Spec[Annotations, TestFailure[Any], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        genCodeFragment: String
    ): ZSpec[Any, Nothing] = genTest2(GenerationPlan.rddPlan)(methodName, arity, args)(genCodeFragment)

    suite("check gen for RDD")(
      checkGen("min")("min(implicit ord: Ordering[T]): Task[T]"),
      checkGen("collect", 0)("collect: Task[Seq[T]]"),
      checkGen("saveAsObjectFile")("saveAsObjectFile(path: String): Task[Unit]"),
      checkGen("countByValue")("Task[Map[T, Long]]"),
      checkGen("map")("map[U: ClassTag](f: T => U): RDD[U]"),
      checkGen("cache")("cache: Task[RDD[T]]"),
      checkGen("dependencies")("dependencies: Task[Seq[Dependency[_]]]"),
      checkGen("zipWithIndex")("zipWithIndex: RDD[(T, Long)]"),
      checkGen("countByValueApprox")("Task[PartialResult[Map[T, BoundedDouble]]]"),
      checkGen("distinct", 2)("distinct(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T]"),
      checkGen("saveAsTextFile", 2)("saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit]")
    )
  }

  val datasetMethods: Spec[Annotations, TestFailure[Any], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1, args: List[String] = Nil)(
        genCodeFragment: String
    ): ZSpec[Any, Nothing] = genTest2(GenerationPlan.datasetPlan)(methodName, arity, args)(genCodeFragment)

    suite("check gen for Dataset")(
      checkGen("filter", 1, List("conditionExpr"))("filter(conditionExpr: String): TryAnalysis[Dataset[T]]"),
      checkGen("orderBy", arity = 1)("_.orderBy(sortExprs: _*)"),
      checkGen("explode", arity = 2)("explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A])")
    )
  }

  override def spec: ZSpec[TestEnvironment, Any] = rddMethods + datasetMethods

}
