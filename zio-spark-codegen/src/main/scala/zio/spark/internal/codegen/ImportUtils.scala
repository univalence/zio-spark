package zio.spark.internal.codegen

object ImportUtils {
  import scala.meta.*

  val importedPackages =
    Seq(
      "scala.collection",
      "scala.reflect",
      "scala.math",
      "scala",
      "java.lang"
    )

  def findAllTypes(methods: Seq[Method]): Seq[String] =
    methods
      .flatMap(m =>
        m.symbol.paramLists.flatten.map(_.typeSignature.dealias.toString) :+ m.symbol.returnType.dealias.toString
      )

  def findImports(methods: Seq[Method]): Map[String, Seq[String]] = {
    val types = findAllTypes(methods).filterNot(_ == "<none>")
    val res   = scala.collection.mutable.Map[String, Seq[String]]()

    types.foreach(_.parse[Type].get.traverse { case t"$pkg.$obj" =>
      res += (pkg.toString() -> res.getOrElse(pkg.toString(), Seq()).:+(obj.toString()).distinct)
    })

    res.toMap
  }

  def generateImport(pkg: String, objs: Seq[String]): String = {
    val underlyingObjects = List("RDD", "Dataset")

    val augmentedObjs =
      objs.map {
        case obj if underlyingObjects.contains(obj) => s"$obj => Underlying$obj"
        case obj                                    => obj
      }

    val strObjs =
      if (augmentedObjs.size == 1 && !augmentedObjs.head.contains("=>")) augmentedObjs.head
      else "{" + augmentedObjs.mkString(", ") + "}"

    s"$pkg.$strObjs"
  }
}
