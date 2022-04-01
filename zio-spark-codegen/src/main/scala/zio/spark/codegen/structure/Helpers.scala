package zio.spark.codegen.structure
import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.plan.SparkPlan

import scala.meta.*

object Helpers {
  def cleanPrefixPackage(type_ : String): String = {

    val res =
      type_
        .parse[Type]
        .get
        .transform {
          case t"Array"                                 => t"Seq"
          case Type.Select(q"scala.collection", tpname) => t"collection.$tpname"
          case t"$ref.$tpname"                          => tpname
        }
    res.toString()
  }

  def cleanType(type_ : String, plan: SparkPlan): String =
    cleanPrefixPackage(type_)
      .replaceAll(",\\b", ", ")
      .replace("this.type", s"${plan.name}${plan.template.typeParameter}")

  def checkMods(mods: List[Mod]): Boolean =
    !mods.exists {
      case mod"@DeveloperApi" => true
      case mod"private[$_]"   => true
      case mod"protected[$_]" => true
      case _                  => false
    }

  def collectFunctionsFromTemplate(template: TemplateWithComments): Seq[Defn.Def] =
    template.stats.collect { case d: Defn.Def if checkMods(d.mods) => d }

  def getTemplateFromSourceOverlay(source: Source): TemplateWithComments =
    new TemplateWithComments(source.children.collectFirst { case c: Defn.Class => c.templ }.get, true)

  def getTemplateFromSource(source: Source): TemplateWithComments =
    new TemplateWithComments(
      source.children
        .flatMap(_.children)
        .collectFirst { case c: Defn.Class => c.templ }
        .get,
      false
    )

  def methodsFromSource(
      source: Source,
      filterOverlay: Boolean,
      hierarchy: String,
      className: String,
      scalaVersion: ScalaBinaryVersion
  ): Seq[Method] = {
    val template: TemplateWithComments =
      if (filterOverlay) getTemplateFromSourceOverlay(source)
      else getTemplateFromSource(source)

    val scalametaMethods = collectFunctionsFromTemplate(template)
    scalametaMethods.map(m => Method.fromScalaMeta(m, template.comments, hierarchy, className, scalaVersion))
  }
}
