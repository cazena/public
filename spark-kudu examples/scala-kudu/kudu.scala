// PART 1: SET UP TABLE

// Step 1
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spa rk.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.kudu.client.CreateTableOptions

// Step 2
val master = "ip-##-###-##-###.cazena.internal:7051"

// Step 3
val kuduContext = new KuduContext(master, sc)

// If multiple masters:
// Step 2
// val master# = "ip-##-###-##-###.cazena.internal:7051"
//
// Case 2: Step 2.5
// val kuduMasters = Seq(master1, master2, master#, ...).mkString(",")
//
// Case2: Step 3
// val kuduContext = new KuduContext(kuduMasters)

// Step 4
var kuduTableName = "insert-name-here"

// Optional Step 4.5
// if (kuduContext.tableExists(kuduTableName)) {
//     kuduContext.deleteTable(kuduTableName)
// }

// Step 5
// Structfield(name, type, nullable?)
val kuduTableSchema = StructType(
    StructField("name", StringType, false) ::
    StructField("age", IntegerType, true) ::
    StructField("city", StringType, true) :: Nil)

// Step 6
val kuduPrimaryKey = Seq("name")

// Step 7
val kuduTableOptions = new CreateTableOption()

// Step 8
kuduTableOptions.setRangePartitionColumns(List("name").asJava).setNumReplicas(3)

// Step 9
kuduContext.createTable(kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)

// PART 2: INSERT DATA


