import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession,Dataset,Row,SaveMode,functions}
import org.apache.spark.sql.types.{StructType, StringType, TimestampType, IntegerType, DoubleType}
import org.apache.log4j.LogManager

object C7 {
    
    def main(args: Array[String]) {

    	val spark = SparkSession.builder().appName("C7").getOrCreate()
        import spark.implicits._
        val log = LogManager.getRootLogger

        val client_orders_schema = (new StructType).add("id", StringType).add("client_id",StringType).add("timestamp", TimestampType).add("product_id",StringType).add("quantity",IntegerType).add("total",DoubleType)
        val clients_schema = (new StructType).add("country", StringType).add("gender",StringType).add("id",StringType).add("name",StringType).add("registration_date", TimestampType)
        val stock_orders_schema = (new StructType).add("id", StringType).add("product_id",StringType).add("quantity",IntegerType).add("timestamp", TimestampType).add("total",DoubleType)

        def saveToFile(ds:Dataset[Row],filename:String) = {
            ds.write.mode(SaveMode.Overwrite)
               .format("com.databricks.spark.csv")
               .option("header", true)
               .option("delimiter", ",")
               .save(s"gs://de-training-output-fherdelpino/assignment-7/$filename")
        }

        val dataSize = "200000"

        val client_orders = spark.read.schema(client_orders_schema).json(s"gs://de-training-input/alimazon/$dataSize/client-orders/*.jsonl.gz")
        val clients = spark.read.schema(clients_schema).json(s"gs://de-training-input/alimazon/$dataSize/clients/*.jsonl.gz")
        val stock_orders = spark.read.schema(stock_orders_schema).json(s"gs://de-training-input/alimazon/$dataSize/stock-orders/*.jsonl.gz")

        log.warn("Files load finished")

        val client_orders_sum = client_orders.groupBy("client_id").agg(
            count("id").alias("total_transactions"),
            sum("quantity").alias("total_products"),
            sum("total").alias("total_amount")
        )

        val client_orders_total = client_orders_sum.orderBy($"total_transactions".desc, $"total_amount".desc).limit(10)

        val client_orders_final = client_orders_total.join(clients.withColumnRenamed("id","client_id"),"client_id").na.fill("",Seq("country"))
        //the columns change the order but also the rows, why???
        //val client_orders_final = clients.withColumnRenamed("id","client_id").join(client_orders_total,"client_id").na.fill("",Seq("country"))

        saveToFile(client_orders_final,"best-clients")
        log.warn("best-clients finished")

        val valid_orders = client_orders.join(clients.withColumnRenamed("id","client_id"),"client_id")

        val products_bought = stock_orders.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)","total_bought").repartition(9,$"product_id")
        val products_sold = valid_orders.groupBy("product_id").sum("quantity").withColumnRenamed("sum(quantity)","total_sold").repartition(9,$"product_id")

        val products_data = products_bought.join(products_sold,"product_id")

        val health_check_report = products_data.withColumn("is_valid", $"total_bought" >= $"total_sold")

        saveToFile(health_check_report,"health-check-report")
    }
}