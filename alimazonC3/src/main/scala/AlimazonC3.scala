/* AlimazonC3.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,Dataset,Row,SaveMode}
import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType}
import java.util.Calendar
import java.sql.Timestamp

object AlimazonC3 {
	
    case class OrderRowSimple(day_of_week:Int, month:Int, client_id:String, product_id:String, total:Double)

    def main(args: Array[String]) {

        val spark = SparkSession.builder().appName("alimazonC3").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext       

        val schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)
       
	    val ordersDF = spark.read.schema(schema).json("gs://de-training-input/alimazon/200000/client-orders/*.jsonl.gz")
        
        def dayOfWeek(t:Timestamp) : Int = {
            val c = Calendar.getInstance()
            c.setTime(t)
            c.get(Calendar.DAY_OF_WEEK)
        }

	    def getMonth(t:Timestamp) : Int = {
            val c = Calendar.getInstance()
            c.setTime(t)
            c.get(Calendar.MONTH)+1
        }

        def saveToFile(ds:Dataset[Row],filename:String) = {
            ds.write.mode(SaveMode.Overwrite)
        	   .format("com.databricks.spark.csv")
        	   .option("header", true)
        	   .option("delimiter", ",")
        	   .save(s"gs://de-training-output-fherdelpino/assignment-3/$filename")
        }

        val ordersDSsimple = ordersDF.map(row => OrderRowSimple(
                dayOfWeek(row.getAs[Timestamp](2)),
                getMonth(row.getAs[Timestamp](2)),
                row.getAs[String](1),
                row.getAs[String](3),
                row.getAs[Double](5)
            )
        )    

        val ordersByWeek1 = ordersDSsimple.filter(r => r.day_of_week == 1)
        val ordersByWeek2 = ordersDSsimple.filter(r => r.day_of_week == 2)
        val ordersByWeek3 = ordersDSsimple.filter(r => r.day_of_week == 3)
        val ordersByWeek4 = ordersDSsimple.filter(r => r.day_of_week == 4)
        val ordersByWeek5 = ordersDSsimple.filter(r => r.day_of_week == 5)
        val ordersByWeek6 = ordersDSsimple.filter(r => r.day_of_week == 6)
        val ordersByWeek7 = ordersDSsimple.filter(r => r.day_of_week == 7)

        val ordersByMonth1 = ordersDSsimple.filter(r => r.month == 1)
        val ordersByMonth2 = ordersDSsimple.filter(r => r.month == 2)
        val ordersByMonth3 = ordersDSsimple.filter(r => r.month == 3)
        val ordersByMonth4 = ordersDSsimple.filter(r => r.month == 4)
        val ordersByMonth5 = ordersDSsimple.filter(r => r.month == 5)
        val ordersByMonth6 = ordersDSsimple.filter(r => r.month == 6)
        val ordersByMonth7 = ordersDSsimple.filter(r => r.month == 7)
        val ordersByMonth8 = ordersDSsimple.filter(r => r.month == 8)
        val ordersByMonth9 = ordersDSsimple.filter(r => r.month == 9)
        val ordersByMonth10 = ordersDSsimple.filter(r => r.month == 10)
        val ordersByMonth11 = ordersDSsimple.filter(r => r.month == 11)
        val ordersByMonth12 = ordersDSsimple.filter(r => r.month == 12)

        //caching
        ordersByWeek1.cache()
        ordersByWeek2.cache()
        ordersByWeek3.cache()
        ordersByWeek4.cache()
        ordersByWeek5.cache()
        ordersByWeek6.cache()
        ordersByWeek7.cache()
        
        ordersByMonth1.cache()
        ordersByMonth2.cache()
        ordersByMonth3.cache()
        ordersByMonth4.cache()
        ordersByMonth5.cache()
        ordersByMonth6.cache()
        ordersByMonth7.cache()
        ordersByMonth8.cache()
        ordersByMonth9.cache()
        ordersByMonth10.cache()
        ordersByMonth11.cache()
        ordersByMonth12.cache()
        //caching end

        //top10_products_by_gross_sales_$i
        saveToFile(
            ordersByWeek1.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_1"
        )            
        saveToFile(
            ordersByWeek2.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_2"
        )
        saveToFile(
            ordersByWeek3.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_3"
        )            
        saveToFile(
            ordersByWeek4.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_4"
        )            
        saveToFile(
            ordersByWeek5.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_5"
        )            
        saveToFile(
            ordersByWeek6.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_6"
        )            
        saveToFile(
            ordersByWeek7.groupBy("product_id").sum("total").withColumnRenamed("sum(total)","gross_sales").orderBy($"gross_sales".desc).limit(10),
            "top10_products_by_gross_sales_7"
        )

        //top10_products_by_orders_$i
        saveToFile(
            ordersByWeek1.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_1"
        )
        saveToFile(
            ordersByWeek2.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_2"
        )
        saveToFile(
            ordersByWeek3.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_3"
        )
        saveToFile(
            ordersByWeek4.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_4"
        )
        saveToFile(
            ordersByWeek5.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_5"
        )
        saveToFile(
            ordersByWeek6.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_6"
        )
        saveToFile(
            ordersByWeek7.groupBy("product_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_products_by_orders_7"
        )

        //top10_customers_by_gross_spending_$i
        saveToFile(
            ordersByMonth1.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_1"
        )
        saveToFile(
            ordersByMonth2.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_2"
        )
        saveToFile(
            ordersByMonth3.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_3"
        )
        saveToFile(
            ordersByMonth4.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_4"
        )
        saveToFile(
            ordersByMonth5.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_5"
        )
        saveToFile(
            ordersByMonth6.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_6"
        )
        saveToFile(
            ordersByMonth7.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_7"
        )
        saveToFile(
            ordersByMonth8.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_8"
        )
        saveToFile(
            ordersByMonth9.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_9"
        )
        saveToFile(
            ordersByMonth10.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_10"
        )
        saveToFile(
            ordersByMonth11.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_11"
        )
        saveToFile(
            ordersByMonth12.groupBy("client_id").sum("total").withColumnRenamed("sum(total)","gross_spending").orderBy($"gross_spending".desc).limit(10),
            "top10_customers_by_gross_spending_12"
        )

        //top10_customers_by_orders_$i
        saveToFile(
            ordersByMonth1.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_1"
        )
        saveToFile(
            ordersByMonth2.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_2"
        )
        saveToFile(
            ordersByMonth3.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_3"
        )
        saveToFile(
            ordersByMonth4.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_4"
        )
        saveToFile(
            ordersByMonth5.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_5"
        )
        saveToFile(
            ordersByMonth6.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_6"
        )
        saveToFile(
            ordersByMonth7.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_7"
        )
        saveToFile(
            ordersByMonth8.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_8"
        )
        saveToFile(
            ordersByMonth9.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_9"
        )
        saveToFile(
            ordersByMonth10.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_10"
        )
        saveToFile(
            ordersByMonth11.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_11"
        )
        saveToFile(
            ordersByMonth12.groupBy("client_id").count().withColumnRenamed("count","orders_count").orderBy($"orders_count".desc).limit(10),
            "top10_customers_by_orders_12"
        )


    }
}
