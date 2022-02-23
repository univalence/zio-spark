package zio.spark.internal.codegen

import zio.test._
import zio.test.TestAspect._

object MethodSpec extends DefaultRunnableSpec {
  val allMethods: Seq[Method] = RDDAnalysis.readMethodsApacheSparkRDD.map(Method.fromSymbol)

  def findMethod(name: String, arity: Int = -1): Option[Method] =
    allMethods.find(m => m.name == name && m.symbol.paramLists.flatten.size == arity) orElse allMethods.find(
      _.name == name
    )

  def genTest(name: String, arity: Int = -1)(generatedCode: String): ZSpec[Any, Nothing] =
    test(name) {
      val find = findMethod(name)
      find.fold(assertNever(s"can't find $name"))(m => assertTrue(m.toCode(RDDAnalysis.getMethodType(m)) == generatedCode))
    }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("basic gen")(
      genTest("saveAsObjectFile")("def saveAsObjectFile(path: String): Task[Unit] = attemptBlocking(_.saveAsObjectFile(path))"),
      genTest("min")("def min(implicit ord: Ordering[T]): Task[T] = attemptBlocking(_.min())"),
      genTest("countByValue")("def countByValue(implicit ord: Ordering[T]): Task[Map[T, Long]] = attemptBlocking(_.countByValue())"),
      genTest("map")("def map[U](f: T => U)(implicit evidence$3: ClassTag[U]): RDD[U] = succeedNow(_.map(f))"),
      genTest("cache")("def cache: Task[RDD[T]] = attemptBlocking(_.cache())") @@ ignore,
      genTest("dependencies")("def dependencies: Task[Seq[Dependency[_]]] = attemptBlocking(_.dependencies)") @@ ignore,
      genTest("zipWithIndex")("def zipWithIndex: RDD[(T, Long)] = succeedNow(_.zipWithIndex())") @@ ignore,
      /* not possible at the moment, we cannot get the information '_ <: CompressionCodec' from the typeTag, it needs to be implemented manually */
      genTest("saveAsTextFile", 2)(
        "def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit] = attemptBlocking(_.saveAsTextFile(path, codec))"
      ) @@ ignore
    )

}
