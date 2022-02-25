package zio.spark.internal.codegen

import zio.test.*
import zio.test.TestAspect.*

object MethodSpec extends DefaultRunnableSpec {
  def genTest2(plan: GenerationPlan[?])(name: String, arity: Int = -1)(generatedCode: String): ZSpec[Any, Nothing] = {
    def findMethod(name: String, arity: Int): Option[Method] =
      plan.methods.find(m => m.name == name && m.symbol.paramLists.flatten.size == arity) orElse plan.methods.find(
        _.name == name
      )

    val maybeMethod = findMethod(name, arity)

    test(name) {
      maybeMethod.fold(assertNever(s"can't find $name"))(m => assertTrue(m.toCode(RDDAnalysis.getMethodType(m, plan.path)).contains(generatedCode)))
    }
  }

  val rddMethods: Spec[Annotations, TestFailure[Any], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1)(genCodeFragment: String): ZSpec[Any, Nothing] =
      genTest2(GenerationPlan.rddPlan)(methodName, arity)(genCodeFragment)

    suite("check gen for RDD")(
      checkGen("collect", 0)("collect: Task[Seq[T]]"),
      checkGen("saveAsObjectFile")("saveAsObjectFile(path: String): Task[Unit]"),
      checkGen("min")("min(implicit ord: Ordering[T]): Task[T]"),
      checkGen("countByValue")("Task[collection.Map[T, Long]]"),
      checkGen("map")("map[U](f: T => U)(implicit evidence$3: ClassTag[U]): RDD[U]"),
      checkGen("cache")("cache: Task[RDD[T]]"),
      checkGen("dependencies")("dependencies: Task[Seq[Dependency[_]]]"),
      checkGen("zipWithIndex")("zipWithIndex: RDD[(T, Long)]"),
      checkGen("countByValueApprox")("Task[PartialResult[collection.Map[T, BoundedDouble]]]"),
      checkGen("distinct", 2)("distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]"),
      /* not possible at the moment, we cannot get the information '_ <: CompressionCodec' from the typeTag, it needs to be implemented manually */
      checkGen("saveAsTextFile", 2)(
        "saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit]"
      ) @@ ignore
    )
  }

  val datasetMethods: Spec[Annotations, TestFailure[Any], TestSuccess] = {
    def checkGen(methodName: String, arity: Int = -1)(genCodeFragment: String): ZSpec[Any, Nothing] =
      genTest2(GenerationPlan.datasetPlan)(methodName, arity)(genCodeFragment)

    suite("check gen for Dataset")(
      checkGen("orderBy", arity = 1)("_.orderBy(sortExprs: _*)"),
      checkGen("explode", arity = 3)(
        "def explode[A <: Product](input: Column*)(f: Row => TraversableOnce[A])(implicit evidence$4: TypeTag[A])"
      ) @@ ignore
    )
  }

  override def spec: ZSpec[TestEnvironment, Any] = rddMethods + datasetMethods

}
