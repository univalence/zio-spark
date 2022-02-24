package zio.spark.internal.codegen

import zio.test.*
import zio.test.TestAspect.*

object MethodSpec extends DefaultRunnableSpec {
  val allMethods: Seq[Method] = RDDAnalysis.readMethodsApacheSparkRDD.map(Method.fromSymbol)

  def findMethod(name: String, arity: Int): Option[Method] =
    allMethods.find(m => m.name == name && m.symbol.paramLists.flatten.size == arity) orElse
      allMethods.find(_.name == name)

  def genTest(name: String, arity: Int = -1)(generatedCode: String): ZSpec[Any, Nothing] =
    test(name) {
      val find = findMethod(name, arity)
      find.fold(assertNever(s"can't find $name"))(m => assertTrue(m.toCode(RDDAnalysis.getMethodType(m)).contains(generatedCode)))
    }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("basic gen")(
      genTest("collect", 0)("collect: Task[Seq[T]]"),
      genTest("saveAsObjectFile")("saveAsObjectFile(path: String): Task[Unit]"),
      genTest("min")("min(implicit ord: Ordering[T]): Task[T]"),
      genTest("countByValue")("countByValue(implicit ord: Ordering[T]): Task[collection.Map[T, Long]]"),
      genTest("map")("map[U](f: T => U)(implicit evidence$3: ClassTag[U]): RDD[U]"),
      genTest("cache")("cache: Task[RDD[T]]"),
      genTest("dependencies")("dependencies: Task[Seq[Dependency[_]]]"),
      genTest("zipWithIndex")("zipWithIndex: RDD[(T, Long)]"),
      genTest("countByValueApprox")("Task[PartialResult[collection.Map[T, BoundedDouble]]]"),
      genTest("distinct", 2)("distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]"),
      /* not possible at the moment, we cannot get the information '_ <: CompressionCodec' from the typeTag, it needs to be implemented manually */
      genTest("saveAsTextFile", 2)(
        "saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit]"
      ) @@ ignore
    )

}
