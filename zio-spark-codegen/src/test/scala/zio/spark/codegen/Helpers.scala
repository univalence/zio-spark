package zio.spark.codegen

import sbt.Keys.Classpath
import sbt.internal.util.Attributed

import zio.{Console, ZIO, ZLayer}
import zio.spark.codegen.generation.Error.CodegenError
import zio.spark.codegen.generation.plan.SparkPlan
import zio.spark.codegen.structure.Method
import zio.test.{TestConsole, TestEnvironment}

import java.io.File
import java.net.URLClassLoader

object Helpers {
  // getClassLoader when running test, instead of using information from sbt
  def classLoaderToClasspath(classLoader: ClassLoader): Classpath =
    classLoader match {
      case classLoader: URLClassLoader => classLoader.getURLs.map(_.getFile).map(x => Attributed.blank(new File(x)))
      case _                           => Seq.empty
    }

  // find a method coming from Spark sources.
  def findMethod(
      name: String,
      arity: Int,
      args: List[String] = Nil
  ): ZIO[Console & SparkPlan & Classpath & ScalaBinaryVersion, CodegenError, Option[Method]] =
    for {
      plan         <- ZIO.service[SparkPlan]
      sparkMethods <- plan.getSparkMethods
      maybeMethod =
        sparkMethods.find { method =>
          val allParams = method.calls.flatMap(_.parameters)
          method.name == name &&
          allParams.size == arity &&
          args.forall(allParams.map(_.name).contains(_))
        }
      backupMaybeMethod = sparkMethods.find(_.name == name)
    } yield maybeMethod orElse backupMaybeMethod

  def findMethodDefault(
      name: String,
      arity: Int,
      args: List[String] = Nil
  ): ZIO[TestEnvironment & SparkPlan, CodegenError, Option[Method]] = {
    val classpathLayer    = ZLayer.succeed(classLoaderToClasspath(this.getClass.getClassLoader))
    val consoleLayer      = TestConsole.silent
    val scalaVersionLayer = ZLayer.succeed(ScalaBinaryVersion.V2_13)
    val layers            = classpathLayer ++ scalaVersionLayer ++ consoleLayer
    findMethod(name, arity, args).provideSomeLayer[TestEnvironment & SparkPlan](layers)
  }

  def planLayer(plan: SparkPlan): ZLayer[Any, Nothing, SparkPlan] = ZLayer.succeed(plan)
}
