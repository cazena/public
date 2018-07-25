// A version with lines ment to be C&P'D into the spark shell
// PART 1: SET UP TABLE

// Step 1
// PART 1 IMPORTS
// NOTE: Copy all the imports and paste it in.
import scala.collection.JavaConverters._, org.apache.spark._, org.apache.spark.SparkContext._, org.apache.kudu.spark.kudu._, org.apache.spark.sql.types.StructType, org.apache.spark.sql.types.StructField, org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.IntegerType, org.apache.kudu.client.CreateTableOptions, org.apache.spark.sql.SQLContext
// PART 2.5 import is the last import ^^^

// Step 2
val master = "ip-##-###-##-###.cazena.internal:7051"

// Step 3
val kuduContext = new KuduContext(master, sc)

// If multiple masters:
// Step 2
// val master# = "ip-##-###-##-###.cazena.internal:7051"
//
// Step 2.5
// val kuduMasters = Seq(master1, master2, master#, ...).mkString(",")
//
// Step 3
// val kuduContext = new KuduContext(kuduMasters)

// Step 4
var kuduTableName = "insert-name-here"

// Optional Step 4.5
// if (kuduContext.tableExists(kuduTableName)){kuduContext.deleteTable(kuduTableName)}

// Step 5
// Structfield(name, type, nullable?)
val kuduTableSchema = StructType( StructField("name", StringType, false) :: StructField("age", IntegerType, true) :: StructField("city", StringType, true) :: Nil)

// Step 6
val kuduPrimaryKey = Seq("name")

// Step 7
val kuduTableOptions = new CreateTableOptions()

// Step 8
kuduTableOptions.setRangePartitionColumns(List("name").asJava).setNumReplicas(3)

// Step 9
kuduContext.createTable(kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)

// PART 1.5: KuduOptions
val kuduOptions: Map[String, String] = Map("kudu.table"-> kuduTableName,"kudu.master" -> master)


// PART 2: INSERT DATA

// Step 1
case class Customer(name:String, age:Int, city:String)

// Step 2
val customers = Array(Customer("name-1", 30, "city-1"), Customer("name-2", 18, "city-2"))

// Step 3
val customersRDD = sc.parallelize(customers)

// Step 4
val customersDF = spark.createDataFrame(customersRDD)

// Step 5
kuduContext.insertRows(customersDF, kuduTableName) 

// OPTIONAL: Step 6
// spark.read.options(kuduOptions).kudu.show()

// PART 2.5: SQLContext
// Import is at the top under `PART 2.5 IMPORTS`
val sqlContext = new SQLContext(sc)

// PART 3: Deleting Data

// Step 1
customersDF.registerTempTable("customers")

// Step 2
val deleteKeysDF = sqlContext.sql("select name from customers where age > 20")

// Step 3
kuduContext.deleteRows(deleteKeyasDF, kuduTableName)

// Optional Step 4
// sqlContext.read.options(kuduOptions).kudu.show

// PART 4: Upsert Data

// Step 1
val newAndChangedCustomers = Array(Customer("name-3", 25, "chicago"), Customer("name-4", 40, "winnipeg"), Customer("name-5", 19, "toronto"))

// Step 2
val newAndChangedRDD = sc.parallelize(newAndChangedCustomers)

// Step 3
val newAndChangedDF = spark.createDataFrame(newAndChangedRDD)

// Step 4
kuduContext.upsertRows(newAndChangedDF, kuduTableName)

// Optional Step 5
// sqlContext.read.options(kuduOptions).kudu.show

// PART 5: Update Data

// Step 1
val modifiedCustomers = Array(Customer("name-5", 25, "chicago"))

// Step 2
val modifiedCustomersRDD = sc.parallelize(modifiedCustomers)

// Step 3
val modifiedCustomersDF = spark.createDataFrame(modifiedCustomersRDD)

// Step 4
kuduContext.updateRows(modifiedCustomersDF, kuduTableName)

// Optional Step 5
// sqlContext.read.options(kuduOptions).kudu.show

// PART 6: Alter Table
// Step 1
import org.apache.kudu.client.KuduClient, org.apache.kudu.client.AlterTableOptions, org.apache.kudu.Type

// Step 2
val client = new KuduClient.KuduClientBuilder("master-ip").defaultAdminOperationTimeoutMs(600000).build()

// Step 3
val o = 0l;

// Step 4
client.alterTable(kuduTableName, new AlterTableOptions().addColumn("column-name", type, o))

// Optional Step 5
// sqlContext.read.options(kuduOptions).kudu.show
