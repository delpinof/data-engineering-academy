import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType, StructField}
import org.apache.spark.sql.{SparkSession,Dataset,SaveMode}
import org.apache.log4j.LogManager

object C5 {
	
	def main(args:Array[String]) {

		val spark = SparkSession.builder().appName("C5").getOrCreate()
        import spark.implicits._

        val log = LogManager.getRootLogger

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

        //default
        log.warn("iniciando particionado default")
		val default_partitions_size = df.mapPartitions(iter => Seq(iter.size).iterator)
		log.warn("iniciando escritura particionado default")
		saveToFile(default_partitions_size,"default-partitions")
		log.warn("terminando escritura particionado default")

		//by id
		log.warn("iniciando particionado por id")
		val df_byId = df.repartition($"id")
		val id_partitions_size = df_byId.mapPartitions(iter => Seq(iter.size).iterator)
		log.warn("iniciando escritura particionado por id")
		saveToFile(id_partitions_size,"id-partitions")
		log.warn("terminando escritura particionado por id")

		//by 15
		log.warn("iniciando particionado por 15")
		val df_by15 = df.repartition(15)
		val partitions_15_size = df_by15.mapPartitions(iter => Seq(iter.size).iterator)
		log.warn("iniciando escritura particionado por 15")
		saveToFile(partitions_15_size,"fifteen-partitions")
		log.warn("terminando escritura particionado por 15")

		//by 15 and id
		log.warn("iniciando particionado por 15 y id")
		val df_by15_byId = df.repartition(15,$"id")
		val df_by15_byId_size = df_by15_byId.mapPartitions(iter => Seq(iter.size).iterator)
		log.warn("iniciando escritura particionado por 15 y id")
		saveToFile(df_by15_byId_size,"fifteen-client_id-partitions")
		log.warn("terminando escritura particionado por 15 y id")


	} //end main
}//end object