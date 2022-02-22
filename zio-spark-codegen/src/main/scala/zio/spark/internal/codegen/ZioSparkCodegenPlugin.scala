package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.*

import zio.spark.internal.codegen.RDDAnalysis.*

import scala.reflect.runtime.universe

object ZioSparkCodegenPlugin extends AutoPlugin {
  object autoImport {
    val sparkLibraryVersion = settingKey[String]("Specifies the version of Spark to depend on")
  }

  private def readFinalClassRDD(scalaSource: File): Set[String] = {
    val file: File = scalaSource / "zio" / "spark" / "rdd" / "RDD.scala"

    import scala.meta.*
    val parsed: Source = IO.read(file).parse[Source].get

    val methods = scala.collection.mutable.TreeSet.empty[String]
    parsed.traverse {
      case m: Decl.Def if !m.mods.contains(Mod.Private) => methods.add(m.name.value)
      case _                                            => Unit
    }
    methods.toSet
  }

  def prefixAllLines(text: String, prefix: String): String = text.split("\n").map(prefix + _).mkString("\n")

  def commentMethods(methods: String, title: String): String =
    s"""/**
       | * $title
       | *
       |${prefixAllLines(methods, " * ")}
       | */""".stripMargin

  override lazy val projectSettings =
    Seq(
      Compile / sourceGenerators += Def.task {
        val file = (Compile / sourceManaged).value / "zio" / "spark" / "internal" / "codegen" / "BaseRDD.scala"

        val zioSparkMethodNames: Set[String] = readFinalClassRDD((Compile / scalaSource).value)
        val apacheSparkMethods: Seq[Method] =
          readMethodsApacheSparkRDD
            .map(Method.fromSymbol)
            .filterNot(_.fullName.contains("$"))
            .filterNot(_.fullName.contains("java.lang.Object"))
            .filterNot(_.fullName.contains("scala.Any"))
            .filterNot(_.fullName.contains("<init>"))

        val body: String =
          apacheSparkMethods
            .groupBy(getMethodType)
            .map { case (methodType, methods) =>
              val allMethods = methods.map(_.toCode(methodType)).mkString("\n")
              methodType match {
                case MethodType.ToImplement => commentMethods(allMethods, "Methods to implement")
                case MethodType.Ignored     => commentMethods(allMethods, "Ignored method")
                case _                      => allMethods
              }
            }
            .mkString("\n\n//===============\n\n")

        IO.write(
          file,
          s"""package zio.spark.internal.codegen
             |
             |import org.apache.spark.rdd.RDD
             |
             |import zio.Task
             |import zio.spark.impure.Impure
             |import zio.spark.impure.Impure.ImpureBox
             |
             |abstract class BaseRDD[T](underlyingDataset: ImpureBox[RDD[T]]) extends Impure[RDD[T]](underlyingDataset) {
             |  import underlyingDataset._
             |
             |${prefixAllLines(body, "  ")}
             |}
             |""".stripMargin
        )
        Seq(file)
      }.taskValue
    )

  /* def generateParamList(paramList: List[universe.Symbol], isParameter: Boolean): String = { val symbolToString =
   * (s: universe.Symbol) => if (isParameter) s"${s.name}: ${s.typeSignature.typeSymbol.name}" else s.name
   *
   * paramList match { case Nil => "" case symbol :: tail if symbol.isImplicit => if (isParameter) { val body = (symbol
   * +: tail).map(symbolToString).mkString(", ") s"(implicit $body)" } else { "" } case symbols => val body =
   * symbols.map(symbolToString).mkString(", ") s"($body)" } }
   *
   * def generateSymbols(paramLists: List[List[universe.Symbol]], isParameter: Boolean): String =
   * paramLists match { case List(Nil) => if (isParameter) "" else "()" case Nil => "" case _ => paramLists.map(l =>
   * generateParamList(l, isParameter)).mkString("") }
   *
   * def generateMethod(method: universe.MethodSymbol, methodType: MethodType): String =
   * methodType match { case MethodType.Ignored => s"[[${method.fullName}]]" case MethodType.ToImplement =>
   * s"[[${method.fullName}]]" case _ => val arguments = generateSymbols(method.paramLists, isParameter = false) val
   * parameters = generateSymbols(method.paramLists, isParameter = true)
   *
   * val returnType =
   * methodType match { case MethodType.DriverAction => s"Task[${method.returnType}]" case
   * MethodType.DistributedComputation => s"Task[${method.returnType}]" case _ => method.returnType.toString }
   *
   * val transformation =
   * methodType match { case MethodType.DriverAction => "attemptBlocking" case MethodType.DistributedComputation =>
   * "attemptBlocking" case _ => "succeedNow" }
   *
   * s"def ${method.name}$parameters: $returnType = $transformation(_.${method.name}$arguments)" } */
}
