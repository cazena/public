import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SQLContext

// Comma-separated list of Kudu masters with port numbers
val master = "ip-10-128-96-197.cazena.internal:7051"
 
// Create an instance of a KuduContext
val kuduContext = new KuduContext(master,sc)

// Specify a table name
var kuduTableName = "spark_kudu_tbl"
 
// Check if the table exists, and drop it if it does
if (kuduContext.tableExists(kuduTableName)) {
 kuduContext.deleteTable(kuduTableName)
}

// 1. Give your table a name
kuduTableName = "spark_kudu_tbl"
 
// 2. Define a schema
val kuduTableSchema = StructType(
StructField("name", StringType , false) ::
StructField("age" , IntegerType, true ) ::
StructField("city", StringType , true ) :: Nil)
 
// 3. Define the primary key
val kuduPrimaryKey = Seq("name")
 
// 4. Specify any further options
val kuduTableOptions = new CreateTableOptions()
kuduTableOptions.setRangePartitionColumns(List("name").asJava).setNumReplicas(3)
 
// 5. Call create table API
kuduContext.createTable(kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)

// Define your case class *outside* your main method
case class Customer(name:String, age:Int, city:String)
 
// Define a list of customers based on the case class already defined above
val customers = Array(Customer("jane", 30, "new york"), Customer("jordan", 18, "toronto"))
 
// Create RDD out of the customers Array
val customersRDD = sc.parallelize(customers)
 
//val customersDF = customersRDD.toDF()
val df = spark.createDataFrame(customersRDD)

// Define Kudu options used by various operations
val kuduOptions: Map[String, String] = Map("kudu.table"-> kuduTableName,"kudu.master" -> master)
 
// Insert our customer DataFrame data set into the Kudu table
kuduContext.insertRows(df, kuduTableName)

// Create a SQL context 
val sqlContext = new SQLContext(sc)

// Read back the records from the Kudu table to see them dumped
spark.read.options(kuduOptions).kudu.show()

// Change log from v1 to v2:
//  - no more SQL context or import needed - use Spark Session accessed via object "spark"
//  - Fixed DataFrame variable typo
//  - deleted redundant definition of kuduTableName
//  - removed "implicits" import