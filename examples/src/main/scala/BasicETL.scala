import org.apache.spark.sql.functions.abs

import zio._
import zio.spark.parameter._
import zio.spark.sql._

object BasicETL extends ZIOAppDefault {
  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

  val inputPath: String  = "examples/src/main/resources/data.csv"
  val outputPath: String = "examples/src/main/resources/output"

  def read: Spark[DataFrame]              = SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv(inputPath)
  def transform(ds: DataFrame): DataFrame = ds.withColumn("yearOfBirth", abs($"age" - 2022))
  def write(ds: DataFrame): Task[Unit]    = ds.write.withHeader.csv(outputPath)

  private val session  = SparkSession.builder.master(localAllNodes).appName("zio-spark").getOrCreateLayer
  val job: Spark[Unit] = Pipeline(read, transform, write).run

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] = job.provideCustomLayer(session)
}
