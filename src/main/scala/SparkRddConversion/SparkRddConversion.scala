package SparkRddConversion


//import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databrics.spark.xml




object SparkRddConversion {
  case class schema(txno:Int,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val schema_type = StructType(Array(
      StructField("txnno",StringType,true),
      StructField("txndate",StringType,true),
      StructField("custno",StringType,true),
      StructField("amount",StringType,true),
      StructField("category",StringType,true),
      StructField("product",StringType,true),
      StructField("city",StringType,true),
      StructField("state",StringType,true),
      StructField("spendby",StringType,true)))

     print("======first way to create DataFrame(Seamless Reads)==========")
    val ftypedf = spark.read.csv("file:///D:/Practice/txns.txt")
    ftypedf.show(5)
    print("=======Second Way (schemaRdd)=====")
    print("read textfile " +
      "do mapsplit " +
      "difine case class and impose schema on rdd " +
      "then concert to Dtaframe using toDF()")
    val data = sc.textFile("file:///D:/Practice/txns.txt")
    val mdata = data.map(x => x.split(","))
    val sdata = mdata.map(x => schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val df = sdata.toDF()
    df.show(10)

    print("=====another way usinf ROW RDD to DataFrame=======")
    val dat = mdata.map(x => Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val df2 = spark.createDataFrame(dat,schema_type)
    df2.show(10)








  }

}
