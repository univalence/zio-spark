package zio.spark.codegen

import sbt.Keys.Classpath
import sbt.internal.util.Attributed

import zio.{Task, UIO, ZLayer}
import zio.spark.codegen.GenerationPlan.PlanType
import zio.spark.codegen.structure.Method

import java.io.File
import java.net.URLClassLoader

object Helpers {
  // getClassLoader when running test, instead of using information from sbt
  def classLoaderToClasspath(classLoader: ClassLoader): Classpath =
    classLoader match {
      case classLoader: URLClassLoader => classLoader.getURLs.map(_.getFile).map(x => Attributed.blank(new File(x)))
      case _                           => Seq.empty
    }

  // Retrieves the GenerationPlan using the Plan Type.
  def getPlan(planType: GenerationPlan.PlanType): Task[GenerationPlan] =
    for {
      classpath <- UIO(classLoaderToClasspath(this.getClass.getClassLoader))
      plan <-
        planType.getGenerationPlan(
          new File(""),
          classpath,
          ScalaBinaryVersion.V2_12
        )
    } yield plan

  // find a method coming from Spark sources.
  def findMethod(name: String, plan: GenerationPlan, arity: Int, args: List[String] = Nil): Option[Method] = {
    val maybeMethod =
      plan.sourceMethods.find { method =>
        val allParams = method.calls.flatMap(_.parameters)
        method.name == name &&
        allParams.size == arity &&
        args.forall(allParams.map(_.name).contains(_))
      }
    maybeMethod orElse plan.sourceMethods.find(_.name == name)
  }

  def planLayer(planType: PlanType): ZLayer[Any, Nothing, GenerationPlan] = getPlan(planType).orDie.toLayer
}
