import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.generic.GenericRecord
import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import org.apache.spark.rdd.RDD



def rddFromParquetHdfsFile(path: String): RDD[GenericRecord] = {
    val job = new Job()
    FileInputFormat.setInputPaths(job, path)
    ParquetInputFormat.setReadSupportClass(job,
        classOf[AvroReadSupport[GenericRecord]])
    return sc.newAPIHadoopRDD(job.getConfiguration,
        classOf[ParquetInputFormat[GenericRecord]],
        classOf[Void],
        classOf[GenericRecord]).map(x => x._2)
}

val warehouse = "hdfs://quickstart/user/hive/warehouse/"
val order_items = rddFromParquetHdfsFile(warehouse + "order_items");
val products = rddFromParquetHdfsFile(warehouse + "products");

val orders = order_items.map { x => (
 x.get("order_item_product_id"),
 (x.get("order_item_order_id"), x.get("order_item_quantity"))
)
}.join(
   products.map { x => (
     x.get("product_id"),
     (x.get("product_name"))
 )
}
).map(x => (
 scala.Int.unbox(x._2._1._1), // order_id
 (
     scala.Int.unbox(x._2._1._2), // quantity
     x._2._2.toString // product_name
 )
)).groupByKey()

val cooccurrences = orders.map(order =>
   (
     order._1,
     order._2.toList.combinations(2).map(order_pair =>
         (
             if (order_pair(0)._2 < order_pair(1)._2)
                 (order_pair(0)._2, order_pair(1)._2)
             else
                 (order_pair(1)._2, order_pair(0)._2),
             order_pair(0)._1 * order_pair(1)._1
         )
     )
 )
)

val combos = cooccurrences.flatMap(x => x._2).reduceByKey((a, b) => a + b)
val mostCommon = combos.map(x => (x._2, x._1)).sortByKey(false).take(10)

println(mostCommon.deep.mkString("\n"))