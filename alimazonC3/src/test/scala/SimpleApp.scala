/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType}
import java.util.Calendar
import java.sql.Timestamp

object SimpleApp {
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("alimazonC3").getOrCreate()
        
        val schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)
        
        //val orders = spark.read.schema(schema).json("part_20180820T154648_00000.jsonl.gz")
        //val orders = spark.read.schema(schema).json(events).select($"action", $"timestamp".cast(LongType)).show

        val orders = spark.read.schema(schema).json("gs://de-training-input/alimazon/200000/client-orders/part_20180820T154648_00000.jsonl.gz")

        orders.createOrReplaceTempView("orders")

        spark.udf.register("dayofweek", (input: Timestamp) => {
            val cal = Calendar.getInstance()
            cal.setTime(input)
            cal.get(Calendar.DAY_OF_WEEK)
        })

        /*
        * V1
        */
        val query = n=>s"select product_id,sum(total) sum_total from orders where dayofweek(timestamp) = $n group by product_id order by sum_total desc limit 10"

        val top10_products_by_gross_sales_1 = spark.sql(query("1"))
        val top10_products_by_gross_sales_2 = spark.sql(query("2"))
        val top10_products_by_gross_sales_3 = spark.sql(query("3"))
        val top10_products_by_gross_sales_4 = spark.sql(query("4"))
        val top10_products_by_gross_sales_5 = spark.sql(query("5"))
        val top10_products_by_gross_sales_6 = spark.sql(query("6"))
        val top10_products_by_gross_sales_7 = spark.sql(query("7"))

        /*
        * V2
        */
        val select = n=>s"select '$n' day_of_week, product_id, sum(total) sum_total "
        val from = n=>s"from orders where dayofweek(timestamp) = $n group by product_id order by sum_total desc limit 10"

        val top10_products_w = spark.sql(select("1").concat(from("1")))

        for (i <- 2 to 7) {
            top10_products_w.union( spark.sql(select(i.toString).concat(from(i.toString))) )
        }


        top10_products_w.write
        	.format("com.databricks.spark.csv")
        	.option("header", false)
        	.option("delimiter", ",")
        	.save("gs://de-training-output-fherdelpino/alimazon-assignment-result")
    }
}