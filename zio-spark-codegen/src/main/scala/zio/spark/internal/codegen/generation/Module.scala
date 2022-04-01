package zio.spark.internal.codegen.generation

/**
 * A module describes a location of the classpath.
 *
 * @param name
 *   The name of the library
 * @param hierarchy
 *   The package hierarchy of the library
 */
case class Module(name: String, hierarchy: String) {

  /** Convert the hierarchy into a stringify path. */
  def path: String = hierarchy.replace(".", "/")

  /** Convert the stringify spark path into a zio spark path. */
  def zioPath: String = path.replace("org/apache/spark", "zio/spark")

  def zioHierarchy: String = hierarchy.replace("org.apache.spark", "zio.spark")
}

object Module {
  val coreModule: Module = Module("spark-core", "org.apache.spark.rdd")
  val sqlModule: Module  = Module("spark-sql", "org.apache.spark.sql")
}
