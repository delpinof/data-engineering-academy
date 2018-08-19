/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}


object SimpleApp {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Matrix Multiplication")
		val sc = new SparkContext(conf)

		val sqlContext= new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._

		val m1s = sc.textFile("gs://de-training-input/matrices/matrix1.txt").toDS
		val m2s = sc.textFile("gs://de-training-input/matrices/matrix2.txt").toDS

    val mEntries1 = m1s.flatMap(row => {
      var c = row.split(",")
      for(i <- 1 until c.size) yield 
        MatrixEntry(c(0).toLong,i-1,c(i).toDouble)
    })

    val mEntries2 = m2s.flatMap(row => {
      var c = row.split(",")
      for(i <- 1 until c.size) yield 
        MatrixEntry(c(0).toLong,i-1,c(i).toDouble)
    })

    def create_blockMatrix(matrixEntries:Dataset[MatrixEntry]) : BlockMatrix = new CoordinateMatrix(matrixEntries.rdd).toBlockMatrix()

    val blockM1 = create_blockMatrix(mEntries1)
    val blockM2 = create_blockMatrix(mEntries2)

    val result = blockM1.multiply(blockM2)

    val bucket = "gs://de-training-output-fherdelpino/matrix-multiplication-assignment-result"
    result.toCoordinateMatrix().entries.saveAsTextFile(bucket)

  }
}