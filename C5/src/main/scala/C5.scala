import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType, StructField}
import org.apache.spark.sql.{Dataset,SaveMode}

object C5 {
	
	def main(args:Array[String]) {

		val struct = StructType(
    		StructField("id", StringType, false) ::
    		StructField("product_id", StringType, false) ::
    		StructField("quantity", IntegerType, false) ::
    		StructField("timestamp", TimestampType, false) ::
    		StructField("total", DoubleType, false) :: Nil)

		def saveToFile(ds:Dataset[Int],filename:String) = {
            ds.write.mode(SaveMode.Overwrite)
               .format("com.databricks.spark.csv")
               .option("header", true)
               .option("delimiter", ",")
               .save(s"gs://de-training-output-fherdelpino/assignment-5/$filename")
        }

        val df = spark.read.schema(struct).json("gs://de-training-input/alimazon/200000/stock-orders")

        df.rdd.getNumPartitions
		df.rdd.partitioner
		df.rdd.glom().collect()

		val default_partitions_size = df.mapPartitions(iter => Seq(iter.size).iterator)

		saveToFile(default_partitions_size,"default-partitions")

	} //end main
}//end object