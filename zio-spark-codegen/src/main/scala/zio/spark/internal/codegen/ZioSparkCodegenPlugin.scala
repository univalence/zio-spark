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
             |import scala.reflect._
             |
             |import org.apache.spark._
             |import org.apache.spark.rdd.{RDD => UnderlyingRDD,_}
             |import org.apache.spark.partial._
             |
             |import zio.Task
             |import zio.spark.impure.Impure
             |import zio.spark.impure.Impure.ImpureBox
             |import zio.spark.rdd.RDD
             |
             |abstract class BaseRDD[T](underlyingRDD: ImpureBox[UnderlyingRDD[T]]) extends Impure[UnderlyingRDD[T]](underlyingRDD) {
             |  import underlyingRDD._
             |
             |  private implicit def arrayToSeq[U](rdd: RDD[Array[U]]): RDD[Seq[U]] = rdd.map(x => x.toSeq)
             |  
             |  /** Applies an action to the underlying RDD. */
             |  def action[U](f: UnderlyingRDD[T] => U): Task[U] = attemptBlocking(f)
             |
             |  /** Applies a transformation to the underlying RDD. */
             |  def transformation[U](f: UnderlyingRDD[T] => UnderlyingRDD[U]): RDD[U] = succeedNow(f.andThen(x => RDD(x)))
             |
             |${prefixAllLines(body, "  ")}
             |}
             |""".stripMargin
        )
        Seq(file)
      }.taskValue
    )
}
