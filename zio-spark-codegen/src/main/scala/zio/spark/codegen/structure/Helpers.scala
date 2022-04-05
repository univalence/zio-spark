package zio.spark.codegen.structure

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.plan.SparkPlan

import scala.meta.*
import scala.meta.contrib.AssociatedComments
import scala.meta.tokens.Token.{KwDef, LF}

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

  def getComments(source: Source): Map[String, String] = {
    val comments = AssociatedComments(source)
    val mapping =
      source.stats.collect { case df: Defn.Def =>
        df.name.toString() -> comments.leading(df).mkString("  ", "\n  ", "")
      }
    mapping.toMap
  }

  def populateComments(stats: List[Tree], comments: Map[String, String]): String =
    stats.map {
      case df: Defn.Def =>
        comments.get(df.name.toString) match {
          case None          => "\n" + df
          case Some(comment) => "\n" + comment + df
        }
      case tree: Tree => populateComments(tree.children, comments)
      case stat       => stat.toString()
    } mkString ""

  /**
   * Merge a specific source into a base source.
   *
   * For each object and class it should add the specific source
   * function into the base source classes.
   *
   * It merges imports.
   */
  def mergeSources(base: Source, specific: Source): String = {
    val stats =
      base.stats.map {
        case clazz: Defn.Class => // Merge specific class functions into base class
          val specificFunctions =
            specific.collect {
              case specificClazz: Defn.Class if s"${specificClazz.name}" == s"${clazz.name}Specific" =>
                specificClazz.templ.stats
            }.flatten

          clazz.copy(templ = clazz.templ.copy(stats = clazz.templ.stats ++ specificFunctions))

        case clazz: Defn.Object => // Merge specific class functions into base class
          val specificFunctions =
            specific.collect {
              case specificClazz: Defn.Object if s"${specificClazz.name}" == s"${clazz.name}Specific" =>
                specificClazz.templ.stats
            }.flatten

          clazz.copy(templ = clazz.templ.copy(stats = clazz.templ.stats ++ specificFunctions))
        case s => s
      }

    val comments = getComments(base) ++ getComments(specific)

    populateComments(stats, comments)
  }
}
