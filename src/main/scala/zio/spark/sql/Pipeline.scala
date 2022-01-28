package zio.spark.sql

import zio._

final case class Pipeline[TIn, TOut, Out](
    input:   SparkSession => Task[Dataset[TIn]],
    process: Dataset[TIn] => Dataset[TOut],
    output:  Dataset[TOut] => Task[Out]
) {
  def run: RIO[SparkSession, Out] =
    for {
      session <- ZIO.service[SparkSession]
      dataset <- input(session)
      processedDataset = process(dataset)
      value <- output(processedDataset)
    } yield value
}

object Pipeline {

  /**
   * Build a pipeline without processing.
   *
   * @param input
   *   The function to create an dataset in input
   * @param output
   *   The function to extract a result from the dataset transformation
   * @tparam TIn
   *   The input type of the dataset
   * @tparam Out
   *   The result type of the pipeline
   * @return
   *   The pipeline description
   */
  def buildWithoutProcessing[TIn, Out](
      input: SparkSession => Task[Dataset[TIn]]
  )(output: Dataset[TIn] => Task[Out]): Pipeline[TIn, TIn, Out] = build(input)(df => df)(output)

  /**
   * Build a pipeline using type inference, you can't use Pipeline case
   * class constructor without specifying each function types since
   * Scala is not capable to find the correct types by itself.
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
   * @return
   *   The pipeline description
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
