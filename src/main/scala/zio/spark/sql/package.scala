package zio.spark

import org.apache.spark.sql.Row

package object sql {
  type DataFrame = Dataset[Row]
}
