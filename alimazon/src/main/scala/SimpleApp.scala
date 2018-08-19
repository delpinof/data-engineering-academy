/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object SimpleApp {
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("alimazon").getOrCreate()
        val orders = spark.read.json("gs://de-training-input/alimazon/50000/client-orders/*.jsonl.gz")
        orders.createOrReplaceTempView("orders")
        val productsCount = spark.sql("select product_id, count(id) as orders_count from orders group by product_id order by orders_count desc")
        productsCount.collect().foreach(println)
    }
}