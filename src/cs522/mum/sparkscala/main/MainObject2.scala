package cs522.mum.sparkscala.main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks

import java.nio.file.{ Files, Paths }

object MainObject2 {

  def main(args: Array[String]) {

    //Create conf object
    val conf = new SparkConf().setAppName("Divya Trips").setMaster("local[2]").set("spark.executor.memory", "1g");

    val sc = new SparkContext(conf)

    def dfSchema(columnNames: List[String]): StructType = StructType(
      Seq(
        StructField(name = "tripid", dataType = IntegerType, nullable = false),
        StructField(name = "starttime", dataType = StringType, nullable = false),
        StructField(name = "stoptime", dataType = StringType, nullable = false),
        StructField(name = "bikeid", dataType = IntegerType, nullable = false),
        StructField(name = "tripduration", dataType = StringType, nullable = false),
        StructField(name = "from_station_id", dataType = IntegerType, nullable = false),
        StructField(name = "from_station_name", dataType = StringType, nullable = false),
        StructField(name = "to_station_id", dataType = IntegerType, nullable = false),
        StructField(name = "to_station_name", dataType = StringType, nullable = false),
        StructField(name = "usertype", dataType = StringType, nullable = true)))

    def row_(line: List[String]): Row = Row(line(0).toInt, line(1), line(2), line(3).toInt, line(4), line(5).toInt, line(6), line(7).toInt, line(8), line(9))

    val rdd: RDD[String] = sc.textFile("input/divyatrips2.txt")

    val headerColumns = rdd.first().split(",").to[List]

    val schema = dfSchema(headerColumns)

    val data = rdd.mapPartitionsWithIndex((index, element) => if (index == 0) element.drop(1) else element) // skip header 
      .map(_.split(",").to[List]).map(row_)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Apply the schema to the RDD.
    val bikeDataFrame = sqlContext.createDataFrame(data, schema)

    // Register the DataFrames as a table.
    bikeDataFrame.registerTempTable("divya_bikes")

    var loop = new Breaks;
    loop.breakable {
      while (true) {
        val input = scala.io.StdIn.readLine("cs522_QL>");
        if (input == "quit") {
          println("Exiting now...");
          loop.break()
        } else {
          val userQuery = sqlContext.sql(input);
          userQuery.show();
        }
      }
    }
  }
}