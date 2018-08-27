/* AlimazonC3v3.scala */
import org.apache.spark.sql.{SparkSession,Dataset,Row,SaveMode}
import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType}
import java.util.Calendar
import java.sql.Timestamp

object AlimazonC3v3 {
    
    def main(args: Array[String]) {
        println("Starting program...")
        val spark = SparkSession.builder().appName("alimazonC3").getOrCreate()
        import spark.implicits._
        
        val schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)

        val orders = spark.read.schema(schema).json("gs://de-training-input/alimazon/200000/client-orders/*.jsonl.gz")

        orders.createOrReplaceTempView("orders")

        println("Defining functions")
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
               .save(s"gs://de-training-output-fherdelpino/assignment-3/$filename" + "-2")
        }

        def runProductReports(i:Int) : Unit = {
            val productWeek = spark.sql(s"select product_id, sum(total) gross_sales, count(id) orders_count from orders where dayofweek(timestamp) = $i group by product_id")
            productWeek.cache()
            saveToFile(productWeek.select("product_id","gross_sales").orderBy($"gross_sales".desc).limit(10),s"top10_products_by_gross_sales_$i")
            saveToFile(productWeek.select("product_id","orders_count").orderBy($"orders_count".desc).limit(10),s"top10_products_by_orders_$i")
            println(s"Product report $i done")
        }

        def runCustomerReports(i:Int) : Unit = {
            val customerMonth = spark.sql(s"select client_id, sum(total) gross_sales, count(id) orders_count from orders where month(timestamp) = $i group by client_id")
            customerMonth.cache()
            saveToFile(customerMonth.select("client_id","gross_sales").orderBy($"gross_sales".desc).limit(10),s"top10_customers_by_gross_spending_$i")
            saveToFile(customerMonth.select("client_id","orders_count").orderBy($"orders_count".desc).limit(10),s"top10_customers_by_orders_$i")
            println(s"Customers report $i done")
        }

        println("Starting reports")
        for (i <- 1 to 7) runProductReports(i)

        for (i <- 1 to 12) runCustomerReports(i)

        println("Program terminated")

    }
}