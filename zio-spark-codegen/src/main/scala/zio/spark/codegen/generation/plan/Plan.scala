package zio.spark.codegen.generation.plan

import zio.{UIO, URIO, ZIO}
import zio.spark.codegen.generation.Environment.{Environment, ScalafmtFormatter}
import zio.spark.codegen.generation.Error.CodegenError
import zio.spark.codegen.generation.Module.{coreModule, sqlModule}
import zio.spark.codegen.generation.Output
import zio.spark.codegen.generation.template.instance.*

import java.nio.file.Path

object Plan {
  val rddPlan: SparkPlan                      = SparkPlan(coreModule, RDDTemplate)
  val datasetPlan: SparkPlan                  = SparkPlan(sqlModule, DatasetTemplate)
  val dataFrameNaFunctionsPlan: SparkPlan     = SparkPlan(sqlModule, DataFrameNaFunctionsTemplate)
  val dataFrameStatFunctionsPlan: SparkPlan   = SparkPlan(sqlModule, DataFrameStatFunctionsTemplate)
  val relationalGroupedDatasetPlan: SparkPlan = SparkPlan(sqlModule, RelationalGroupedDatasetTemplate)
  val keyValueGroupedDatasetPlan: SparkPlan   = SparkPlan(sqlModule, KeyValueGroupedDatasetTemplate)
}

/**
 * Abstract the process of generation that all plan should respect to
 * work.
 */
trait Plan {
  val name: String

  def generatePath: URIO[Environment, Path]

  def generateCode: ZIO[Environment, CodegenError, String]

  def formatCode(code: String): URIO[Environment, String] =
    for {
      scalafmt <- ZIO.service[ScalafmtFormatter]
      path     <- generatePath
    } yield scalafmt.format(code, path)

  def preValidation: ZIO[Environment, CodegenError, Unit]                = UIO.unit
  def postValidation(code: String): ZIO[Environment, CodegenError, Unit] = UIO.unit

  def generate: ZIO[Environment, CodegenError, Output] =
    for {
      _    <- preValidation
      path <- generatePath
      raw  <- generateCode
      base =
        s"""/**
           | * /!\\ Warning /!\\
           | *
           | * This file is generated using zio-spark-codegen, you should not edit
           | * this file directly.
           | */
           | 
           |$raw""".stripMargin
      code <- formatCode(base)
      _    <- postValidation(code)
    } yield Output(path.toFile, code)
}
