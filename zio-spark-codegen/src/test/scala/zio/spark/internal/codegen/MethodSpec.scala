package zio.spark.internal.codegen

import zio.test.{assertNever, assertTrue, ignored, Assertion, DefaultRunnableSpec, TestEnvironment, ZSpec}

object MethodSpec extends DefaultRunnableSpec {
  val allMethods: Seq[Method] = RDDAnalysis.readMethodsApacheSparkRDD.map(Method.fromSymbol)

  def findMethod(name: String, arity: Int = -1): Option[Method] =
    allMethods.find(m => m.name == name && m.symbol.paramLists.flatten.size == arity) orElse allMethods.find(
      _.name == name
    )

  def genTest(name: String, arity: Int = -1)(generatedCode: String): ZSpec[Any, Nothing] =
    test(name) {
      val find = findMethod(name)
      find.fold(assertNever(s"can't find $name"))(m =>
        assertTrue(m.toCode(RDDAnalysis.getMethodType(m)) == generatedCode)
      )
    }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("basic gen")(
      genTest("saveAsObjectFile")(
        "def saveAsObjectFile(path: String): Task[Unit] = attemptBlocking(_.saveAsObjectFile(path))"
      ),
      /* not possible at the moment, we cannot get the information '_ <: CompressionCodec' from the typeTag, it needs to
       * be implemented manually */
      /* genTest("saveAsTextFile", 2)("def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit]
       * = attemptBlocking(_.saveAsTextFile(path, codec))") */
      genTest("min")("def min(implicit ord: Ordering[T]): Task[T] = attemptBlocking(_.min())"),
      genTest("countByValue")(
        "def countByValue(implicit ord: Ordering[T]): Task[Map[T, Long]] = attemptBlocking(_.countByValue())"
      )
      /* genTest("map")( "def map[U: ClassTag](f: T => U): RDD[U] = succeedNow(_.map(f))" ) */
    )

}
