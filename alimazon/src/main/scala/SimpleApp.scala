/* SimpleApp.scala */
import org.apache.spark.sql.{SaveMode,SparkSession}

object SimpleApp {
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("alimazon").getOrCreate()
        val orders = spark.read.json("/home/fherdelpino/Downloads/alimazon/*.jsonl.gz")
        orders.createOrReplaceTempView("orders")
        val productsCount = spark.sql("select product_id, count(id) as orders_count from orders group by product_id order by orders_count desc")
        productsCount.write.mode(SaveMode.Overwrite)
        	.format("com.databricks.spark.csv")
        	.option("header", false)
        	.option("delimiter", ",")
        	.save("/home/fherdelpino/Downloads/alimazon/alimazon-assignment-result2")
    }
}
