/* SimpleApp.scala */
import org.apache.spark.sql.{SparkSession,Dataset,Row,SaveMode}
import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType}
import java.util.Calendar
import java.sql.Timestamp

object SimpleApp {
    
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("alimazonC3").getOrCreate()
        import spark.implicits._
        
        val schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)

        val orders = spark.read.schema(schema).json("gs://de-training-input/alimazon/200000/client-orders/*.jsonl.gz")

        orders.createOrReplaceTempView("orders")

        spark.udf.register("dayofweek", (input: Timestamp) => {
            val cal = Calendar.getInstance()
            cal.setTime(input)
            cal.get(Calendar.DAY_OF_WEEK)
        })

        //Function to save to a file in CVS format
        def saveToFile(ds:Dataset[Row],filename:String) = {
            ds.write.mode(SaveMode.Overwrite)
               .format("com.databricks.spark.csv")
               .option("header", true)
               .option("delimiter", ",")
               .save(s"gs://de-training-output-fherdelpino/assignment-3/$filename")
        }

        val productWeek1 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 1 group by product_id")
        val productWeek2 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 2 group by product_id")
        val productWeek3 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 3 group by product_id")
        val productWeek4 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 4 group by product_id")
        val productWeek5 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 5 group by product_id")
        val productWeek6 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 6 group by product_id")
        val productWeek7 = spark.sql("select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = 7 group by product_id")

        val productMonth1 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 1 group by client_id")
        val productMonth2 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 2 group by client_id")
        val productMonth3 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 3 group by client_id")
        val productMonth4 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 4 group by client_id")
        val productMonth5 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 5 group by client_id")
        val productMonth6 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 6 group by client_id")
        val productMonth7 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 7 group by client_id")
        val productMonth8 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 8 group by client_id")
        val productMonth9 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 9 group by client_id")
        val productMonth10 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 10 group by client_id")
        val productMonth11 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 11 group by client_id")
        val productMonth12 = spark.sql("select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = 12 group by client_id")

        productWeek1.cache()
        productWeek2.cache()
        productWeek3.cache()
        productWeek4.cache()
        productWeek5.cache()
        productWeek6.cache()
        productWeek7.cache()

        productMonth1.cache()
        productMonth2.cache()
        productMonth3.cache()
        productMonth4.cache()
        productMonth5.cache()
        productMonth6.cache()
        productMonth7.cache()
        productMonth8.cache()
        productMonth9.cache()
        productMonth10.cache()
        productMonth11.cache()
        productMonth12.cache()

        saveToFile(productWeek1.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_1")
        saveToFile(productWeek2.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_2")
        saveToFile(productWeek3.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_3")
        saveToFile(productWeek4.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_4")
        saveToFile(productWeek5.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_5")
        saveToFile(productWeek6.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_6")
        saveToFile(productWeek7.orderBy($"gross_sales".desc).limit(10),"top10_products_by_gross_sales_7")


        saveToFile(productWeek1.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_1")
        saveToFile(productWeek2.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_2")
        saveToFile(productWeek3.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_3")
        saveToFile(productWeek4.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_4")
        saveToFile(productWeek5.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_5")
        saveToFile(productWeek6.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_6")
        saveToFile(productWeek7.orderBy($"orders_count".desc).limit(10),"top10_products_by_orders_7")

        saveToFile(productMonth1.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_1")
        saveToFile(productMonth2.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_2")
        saveToFile(productMonth3.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_3")
        saveToFile(productMonth4.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_4")
        saveToFile(productMonth5.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_5")
        saveToFile(productMonth6.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_6")
        saveToFile(productMonth7.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_7")
        saveToFile(productMonth8.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_8")
        saveToFile(productMonth9.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_9")
        saveToFile(productMonth10.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_10")
        saveToFile(productMonth11.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_11")
        saveToFile(productMonth12.orderBy($"gross_sales".desc).limit(10),"top10_customers_by_gross_spending_12")

        saveToFile(productMonth1.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_1")
        saveToFile(productMonth2.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_2")
        saveToFile(productMonth3.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_3")
        saveToFile(productMonth4.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_4")
        saveToFile(productMonth5.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_5")
        saveToFile(productMonth6.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_6")
        saveToFile(productMonth7.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_7")
        saveToFile(productMonth8.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_8")
        saveToFile(productMonth9.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_9")
        saveToFile(productMonth10.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_10")
        saveToFile(productMonth11.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_11")
        saveToFile(productMonth12.orderBy($"orders_count".desc).limit(10),"top10_customers_by_orders_12")

    }
}