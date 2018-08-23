/* AlimazonC3v2.scala */
import spark.implicits._
import java.util.Calendar
import java.sql.Timestamp

object AlimazonC3v2 {
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("alimazonC3").getOrCreate()

        val schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)
       
        val ordersDF = spark.read.schema(schema).json("part_20180820T154648_00000.jsonl.gz")
        val ordersDF = spark.read.schema(schema).json("gs://de-training-input/alimazon/200000/client-orders/part_20180820T154648_00000.jsonl.gz")
        
        def dayOfWeek(t:Timestamp) : Int = {
            val c = Calendar.getInstance()
            c.setTime(t)
            c.get(Calendar.DAY_OF_WEEK)
        }

        case class orderRow(id:String,day_of_week:Int, product_id:String, total:Double)

        val ordersDS = ordersDF.map(row => orderRow(
                row.getAs[String](0),
                dayOfWeek(row.getAs[Timestamp](2)),
                row.getAs[String](3),
                row.getAs[Double](5)
            )
        )
        
        //val products_by_day_ordered = ordersDS.groupBy("day_of_week","product_id").sum("total").orderBy($"day_of_week",$"sum(total)".desc)

        val top10_products_by_gross_sales_1 = ordersDS.filter(r => r.day_of_week == 1).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)
        val top10_products_by_gross_sales_2 = ordersDS.filter(r => r.day_of_week == 2).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)
        val top10_products_by_gross_sales_3 = ordersDS.filter(r => r.day_of_week == 3).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)
        val top10_products_by_gross_sales_4 = ordersDS.filter(r => r.day_of_week == 4).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)
        val top10_products_by_gross_sales_5 = ordersDS.filter(r => r.day_of_week == 5).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)
        val top10_products_by_gross_sales_6 = ordersDS.filter(r => r.day_of_week == 6).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)
        val top10_products_by_gross_sales_7 = ordersDS.filter(r => r.day_of_week == 7).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10)

        val top10_products_by_orders_1 = ordersDS.filter(r => r.day_of_week == 1).groupBy("product_id").count().orderBy($"count".desc).limit(10)
        val top10_products_by_orders_2 = ordersDS.filter(r => r.day_of_week == 2).groupBy("product_id").count().orderBy($"count".desc).limit(10)
        val top10_products_by_orders_3 = ordersDS.filter(r => r.day_of_week == 3).groupBy("product_id").count().orderBy($"count".desc).limit(10)
        val top10_products_by_orders_4 = ordersDS.filter(r => r.day_of_week == 4).groupBy("product_id").count().orderBy($"count".desc).limit(10)
        val top10_products_by_orders_5 = ordersDS.filter(r => r.day_of_week == 5).groupBy("product_id").count().orderBy($"count".desc).limit(10)
        val top10_products_by_orders_6 = ordersDS.filter(r => r.day_of_week == 6).groupBy("product_id").count().orderBy($"count".desc).limit(10)
        val top10_products_by_orders_7 = ordersDS.filter(r => r.day_of_week == 7).groupBy("product_id").count().orderBy($"count".desc).limit(10)

        def saveToFile(ds:Dataset,filename:String) : = {
            ds.write
        	   .format("com.databricks.spark.csv")
        	   .option("header", true)
        	   .option("delimiter", ",")
        	   .save(s"de-training-output-fherdelpino/assignment-3/$filename")
        }

        saveToFile(ordersDS.filter(r => r.day_of_week == 1).groupBy("product_id").sum("total").orderBy($"sum(total)".desc).limit(10),
            "top10_products_by_gross_sales_1"
        )
    }
}