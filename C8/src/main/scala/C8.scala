import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession,SaveMode}
import scala.collection.mutable.ListBuffer

object C8 {

	case class c7Metadata(magic:String,width:String,height:String,max_value:String)
    
    def main(args: Array[String]) {

    	val spark = SparkSession.builder().appName("C8").getOrCreate()
        import spark.implicits._

        def b64ToMetadata: (String => c7Metadata) = { src =>
    		val base64Decoded = java.util.Base64.getDecoder.decode(src.getBytes())
    		val ascii = new String(base64Decoded)
    
    		var words = new ListBuffer[String]()
    		val lines = ascii.split("\n")
    		var i = 0
    		while(words.length < 4) {
        		val comments = lines(i).takeWhile(_ != '#')
        		if (comments.length > 0) {
            		for (word <- comments.split(" ")) {
                		words += word
            		}
        		}
        		i+=1
	    	}
	    	c7Metadata(words(0),words(1),words(2),words(3))
		}

		val b64ToMetadataUDF = udf(b64ToMetadata)

		val addressType = StructType(List(
		    StructField("streetAddress",StringType,false),
		    StructField("city",StringType,false),
		    StructField("state",StringType,false),
		    StructField("postalCode",LongType,false)
		))

		val phoneNumbersType = ArrayType(StructType(List(
		    StructField("type",StringType,false),
		    StructField("number",StringType,false)
		)))

		val imageType = StructType(List(
		    StructField("data",StringType,false)
		))

		val schema = StructType(List(
		    StructField("firstName",StringType,false),
		    StructField("lastName",StringType,false),
		    StructField("isAlive",BooleanType,false),
		    StructField("age",IntegerType,false),
		    StructField("address",addressType,false),
		    StructField("phoneNumbers",phoneNumbersType,false),
		    StructField("image",imageType,false)
		))

		val profiles = spark.read.schema(schema).json("gs://de-training-input/profiles")

		val profiles_metadata = profiles.withColumn(
    		"image2",
    		struct(
        		col("image.data").as("data"),
        		b64ToMetadataUDF($"image.data").as("metadata")
    		)
		)

		val profiles_metadata_final = profiles_metadata.drop("image").withColumnRenamed("image2","image")

		profiles_metadata_final.write.mode(SaveMode.Overwrite)
        	   .json(s"gs://de-training-output-fherdelpino/assignment-8/profiles-image-metadata")

    }//end def
}//end object