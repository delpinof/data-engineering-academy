import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, DoubleType, StringType, StructField, StructType}
import org.apache.log4j.LogManager

object C6 {
	def main(args: Array[String]) {

		val spark = SparkSession.builder().appName("C6").getOrCreate()
		import spark.implicits._

		val log = LogManager.getRootLogger

		val bucket = "de-training-output-fherdelpino"
		val destFolder = "assignment-6"

		val orders = spark.read.json("gs://de-training-input/alimazon/200000/stock-orders/")

		val products = orders.groupBy(year($"timestamp").alias("year"), weekofyear($"timestamp").alias("week"), substring($"id",0,2).alias("category"), $"product_id").agg(sum($"total").alias("total"), sum($"quantity").alias("quantity")).cache


		/*****************************Top 5 Most Purchased Products, 34s ***********************************/

		val windowDesc = Window.partitionBy($"year", $"week", $"category").orderBy(desc("total"))

		//Don't repartition
		//val orderedProductsDesc = products.withColumn("order", row_number over windowDesc).repartition(500)
		val orderedProductsDesc = products.withColumn("order", row_number over windowDesc)

		//avoid RDD
		//val topMostProducts = orderedProductsDesc.rdd.filter(row => row.getInt(6) <= 5)
		//val schema = new StructType().add(StructField("year", IntegerType, true)).add(StructField("week", IntegerType, true)).add(StructField("category", StringType, true)).add(StructField("product_id", StringType, true)).add(StructField("total", DoubleType, true)).add(StructField("quantity", LongType, true)).add(StructField("order", IntegerType, true))
		//val topMostProductsDF = spark.createDataFrame(topMostProducts, schema).cache
		val topMostProductsDF = orderedProductsDesc.filter($"order" <= 5).cache

		//TODO: a repartition might be needed

		val topMostTotals = topMostProductsDF.groupBy($"year", $"week".alias("week_num"), $"category".alias("prod_cat")).agg(sum($"quantity").alias("total_qty_top5"), sum($"total").alias("total_spent_top5"))

		//coalesce is needed!!!
		topMostTotals.coalesce(1).write.mode(SaveMode.Overwrite).option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(s"gs://$bucket/$destFolder/top-5-most")

		/***********************Top 5 Least Purchased Products, 56s *************************************/

		val windowAsc = Window.partitionBy($"year", $"week", $"category").orderBy(asc("total"))

		//Don't repartition
		//val orderedProductsAsc = products.withColumn("order", row_number over windowAsc).repartition(500)
		val orderedProductsAsc = products.withColumn("order", row_number over windowAsc)

		//avoid RDD
		//val topLeastProducts = orderedProductsAsc.rdd.filter(row => row.getInt(6) <= 5)
		//val topLeastProductsDF = spark.createDataFrame(topLeastProducts, schema).cache
		val topLeastProductsDF = orderedProductsAsc.filter($"order" <= 5).cache

		//TODO: a repartition might be needed

		val topLeastTotals = topLeastProductsDF.groupBy($"year", $"week".alias("week_num"), $"category".alias("prod_cat")).agg(sum($"quantity").alias("total_qty_top5"), sum($"total").alias("total_spent_top5"))

		topLeastTotals.coalesce(1).write.mode(SaveMode.Overwrite).option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(s"gs://$bucket/$destFolder/top-5-least")


		/*****************************Categories, 1m 24s *******************************************/

		//repartition to avoid empty partitions
		//val topMostProductsConcat = topMostProductsDF.groupBy($"category").pivot("order").agg(first("product_id")).withColumn("top5_most", concat_ws(";", $"1", $"2", $"3", $"4", $"5")).drop("1","2","3","4","5")
		val topMostProductsConcat = topMostProductsDF.groupBy($"category").pivot("order").agg(first("product_id")).withColumn("top5_most", concat_ws(";", $"1", $"2", $"3", $"4", $"5")).drop("1","2","3","4","5").repartition(9)

		//repartition to avoid empty partitions
		//val topLeastProductsConcat = topLeastProductsDF.groupBy($"category").pivot("order").agg(first("product_id")).withColumn("top5_least", concat_ws(";", $"1", $"2", $"3", $"4", $"5")).drop("1","2","3","4","5")
		val topLeastProductsConcat = topLeastProductsDF.groupBy($"category").pivot("order").agg(first("product_id")).withColumn("top5_least", concat_ws(";", $"1", $"2", $"3", $"4", $"5")).drop("1","2","3","4","5").repartition(9)

		val categoryTotals = orders.groupBy(substring($"id",0,2).alias("category")).agg(sum($"total").alias("total_spent"), sum($"quantity").alias("total_qty_cat"))

		val allTotals = categoryTotals.join(topMostProductsConcat, "category").join(topLeastProductsConcat, "category")

		val allTotalsOrdered = allTotals.select($"category",$"top5_most", $"top5_least", $"total_qty_cat", $"total_spent")

		allTotalsOrdered.coalesce(1).write.mode(SaveMode.Overwrite).option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(s"gs://$bucket/$destFolder/top-5-all")		
	}
}