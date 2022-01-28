package zio.spark.sql

import zio._

/**
 * A class representing a Spark pipeline. Generally speaking, a Spark
 * pipeline can be divided in three components:
 *   - We load a dataframe from an external source => The input function
 *   - We transform this dataframe using Spark => The process function
 *   - We do something with the output => The output function
 *
 * @param input
 *   The function to create an dataset in input
 * @param process
 *   The whole dataset processing
 * @param output
 *   The function to extract a result from the dataset transformation
 * @tparam TIn
 *   The input type of the dataset
 * @tparam TOut
 *   The output type of the dataset
 * @tparam Out
 *   The result type of the pipeline
 */
final case class Pipeline[TIn, TOut, Out](
    input:   SparkSession => Task[Dataset[TIn]],
    process: Dataset[TIn] => Dataset[TOut],
    output:  Dataset[TOut] => Task[Out]
) {

  /** Run a pipeline and output the action value */
  def run: RIO[SparkSession, Out] =
    for {
      session <- ZIO.service[SparkSession]
      dataset <- input(session)
      processedDataset = process(dataset)
      value <- output(processedDataset)
    } yield value
}

object Pipeline {

  /** Build a pipeline without processing. */
  def buildWithoutProcessing[TIn, Out](
      input: SparkSession => Task[Dataset[TIn]]
  )(output: Dataset[TIn] => Task[Out]): Pipeline[TIn, TIn, Out] = build(input)(df => df)(output)

  /**
   * Build a pipeline using type inference, you can't use Pipeline case
   * class constructor without specifying each function types since
   * Scala is not capable to find the correct types by itself.
   */
  def build[TIn, TOut, Out](
      input: SparkSession => Task[Dataset[TIn]]
  )(process: Dataset[TIn] => Dataset[TOut])(output: Dataset[TOut] => Task[Out]): Pipeline[TIn, TOut, Out] =
    Pipeline(
      input   = input,
      process = process,
      output  = output
    )
}
