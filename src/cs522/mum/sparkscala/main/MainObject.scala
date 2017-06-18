package cs522.mum.sparkscala.main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row

object MainObject {

  def main(args: Array[String]) {

    //Create conf object
    val conf = new SparkConf().setAppName("Divya Trips").setMaster("local[2]").set("spark.executor.memory", "1g");

    val sc = new SparkContext(conf)

    // Create an RDD
    val bikes = sc.textFile("input/divyatrips.txt")

    // The schema is encoded in a string
    val schemaString = "id starttime stoptime bikeid tripduration from_station_id from_station_name to_station_id to_station_name usertype"

    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //    schema.foreach(println)

    // Convert records of the RDD (fruit) to Rows.
    val rowRDD = bikes
      .map(_.split(","))
      .map(dt => Row(dt(0), dt(1), dt(2), dt(3), dt(4), dt(5), dt(6), dt(7), dt(8), dt(9)));

    //    rowRDD.foreach(println)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Apply the schema to the RDD.
    val bikeDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    bikeDataFrame.registerTempTable("divya_bikes")

    val results = sqlContext.sql("SELECT * FROM divya_bikes")
    results.limit(10).show()

    val results1 = sqlContext.sql("SELECT starttime,stoptime,bikeid, tripduration, from_station_name, to_station_name FROM divya_bikes where tripduration>400 and usertype=\"Customer\"");
    results1.show();  //By default is 20 rows
    
    val totalRecords = sqlContext.sql("SELECT * FROM divya_bikes WHERE usertype=\"Subscriber\"");
    val c = totalRecords.collect()
    val value = sc.parallelize(c)
    value.saveAsTextFile("output/an")
  }
}