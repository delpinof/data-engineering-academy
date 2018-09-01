
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, LongType, DoubleType, StringType, StructField, StructType}

// don't forget to set the correct destination bucket
val bucket = "de-training-output-fherdelpino"

val orders = spark.read.json("gs://de-training-input/alimazon/200000/stock-orders/")

val products = orders.groupBy(year($"timestamp").alias("year"), weekofyear($"timestamp").alias("week"), substring($"id",0,2).alias("category"), $"product_id").agg(sum($"total").alias("total"), sum($"quantity").alias("quantity"))

val windowDesc = Window.partitionBy($"year", $"week", $"category").orderBy(desc("total"))

val orderedProductsDesc = products.withColumn("order", row_number over windowDesc).repartition(500)

val topMostProducts = orderedProductsDesc.rdd.filter(row => row.getInt(6) <= 5)

val schema = new StructType().add(StructField("year", IntegerType, true)).add(StructField("week", IntegerType, true)).add(StructField("category", StringType, true)).add(StructField("product_id", StringType, true)).add(StructField("total", DoubleType, true)).add(StructField("quantity", LongType, true)).add(StructField("order", IntegerType, true))

val topMostProductsDF = spark.createDataFrame(topMostProducts, schema).cache

val topMostTotals = topMostProductsDF.groupBy($"year", $"week".alias("week_num"), $"category".alias("prod_cat")).agg(sum($"quantity").alias("total_qty_top5"), sum($"total").alias("total_spent_top5"))

topMostTotals.coalesce(1).write.option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(s"gs://$bucket/assignment-6/top-5-most")

val windowAsc = Window.partitionBy($"year", $"week", $"category").orderBy(asc("total"))

val orderedProductsAsc = products.withColumn("order", row_number over windowAsc).repartition(500)

val topLeastProducts = orderedProductsAsc.rdd.filter(row => row.getInt(6) <= 5)

val topLeastProductsDF = spark.createDataFrame(topLeastProducts, schema).cache

val topLeastTotals = topLeastProductsDF.groupBy($"year", $"week".alias("week_num"), $"category".alias("prod_cat")).agg(sum($"quantity").alias("total_qty_top5"), sum($"total").alias("total_spent_top5"))

topLeastTotals.coalesce(1).write.option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(s"gs://$bucket/assignment-6/top-5-least")

val topMostProductsConcat = topMostProductsDF.groupBy($"category").pivot("order").agg(first("product_id")).withColumn("top5_most", concat_ws(";", $"1", $"2", $"3", $"4", $"5")).drop("1","2","3","4","5")

val topLeastProductsConcat = topLeastProductsDF.groupBy($"category").pivot("order").agg(first("product_id")).withColumn("top5_least", concat_ws(";", $"1", $"2", $"3", $"4", $"5")).drop("1","2","3","4","5")

val categoryTotals = orders.groupBy(substring($"id",0,2).alias("category")).agg(sum($"total").alias("total_spent"), sum($"quantity").alias("total_qty_cat")).cache

val allTotals = categoryTotals.join(topMostProductsConcat, "category").join(topLeastProductsConcat, "category")

val allTotalsOrdered = allTotals.select($"category",$"top5_most", $"top5_least", $"total_qty_cat", $"total_spent")

allTotalsOrdered.coalesce(1).write.option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(s"gs://$bucket/assignment-6/top-5-all")




