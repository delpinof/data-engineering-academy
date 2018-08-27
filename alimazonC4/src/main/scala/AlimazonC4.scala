import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,Dataset,Row,SaveMode,functions}
import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType}

object AlimazonC4 {
    
    def main(args: Array[String]) {

        val spark = SparkSession.builder().appName("alimazonC3").getOrCreate()
        import spark.implicits._
        //val sc = spark.sparkContext

        val schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)
        val orders = spark.read.schema(schema).json("gs://de-training-input/alimazon/200000/client-orders/*.jsonl.gz")
        orders.createOrReplaceTempView("orders")

        def saveToFile(ds:Dataset[Row],filename:String) = {
            ds.write.mode(SaveMode.Overwrite)
               .format("com.databricks.spark.csv")
               .option("header", true)
               .option("delimiter", ",")
               .save(s"gs://de-training-output-fherdelpino/assignment-4/$filename")
        }

        val best_selling_hours = spark.sql("select hour(timestamp) as hour, bround(avg(total),2) average_spending, min(total) min_spending, max(total) max_spending from orders group by hour order by average_spending")

        saveToFile(best_selling_hours,"assignment-4-best_selling_hours-1")

        val monthly_sales = spark.sql("select product_id, sum(quantity) total_products_sold, sum(total) total_registered_sales from orders where months_between(current_date,timestamp) <= 6 group by product_id order by total_products_sold desc limit 10")

        val monthly_disscount = monthly_sales
                            .withColumn("unit_product_price",org.apache.spark.sql.functions.bround($"total_registered_sales"/$"total_products_sold",2))
                            .withColumn("new_unit_product_price",org.apache.spark.sql.functions.bround($"unit_product_price"*0.9,2))                            

        saveToFile(monthly_disscount,"assignment-4-monthly_disscount-1")

        val client_products = spark.sql("select client_id, sum(quantity) products_count from orders group by client_id")

        val client_orders_dist = client_products.groupBy("products_count").count().withColumnRenamed("count","clients_count").orderBy($"clients_count")

        saveToFile(client_orders_dist,"assignment-4-client_orders_dist-1")

    }
}