# Scala & Kudu Example

## Part 1: Setting Up the Table

### **Step 1:** Import dependencies

Import all the following dependences to create be able to create a table:
```scala
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spa rk.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.kudu.client.CreateTableOptions
```

### **Step 2:** Set up Kudu masters (1 master)

If there is only 1 master, replace the `#` with the IP. If you have more than one master, look at the alternate steps below Step 3
```scala
val master = "ip-##-###-##-###.cazena.internal:7051"
```

If there are *more than 1* masters than create multiple variables like below, replacing `#` with a different number for each master
```scala
val master# = "ip-##-###-##-###.cazena.internal:7051"
```

### **Step 3:** Create A KuduContext (1 master)
```scala
val kuduContext = new KuduContext(master, sc)
```

### Alternate for 1+ Masters (Steps 2-3)
Set up the kudu masters. Include all of the masters in the `Seq(master1, master2,...)`
```scala
val kuduMasters = Seq(master1, master2, ...).mkString(",")
```
Next create a KuduContext:
```scala
val kuduContext = new KuduContext(kuduMasters)
```
Continue as normal

### **Step 4**: Create the kudu table's name
Insert the name of the table in `insert-name-here`
```scala
var kuduTableName = "insert-name-here"
```

### **Optional Step 4.5**: Check for exisiting table
Check if the table already exsists, and if so delete it
```scala
if (kuduContext.tableExists(kuduTableName)) {
     kuduContext.deleteTable(kuduTableName)
}
```

### **Step 5**: Create the schema
Enter as many fields as needed. Format goes `name, type, nullable?`. Types are SQL types and need to be imported induvidually
```scala
val kuduTableSchema = StructType(
    StructField("name", StringType, false) ::
    StructField("age", IntegerType, true) ::
    StructField("city", StringType, true) :: Nil)
```

### **Step 6**: Define the primary key
The primary key matches one of the names in the schema
```scala
val kuduPrimaryKey = Seq("name")
```

### **Step 7**: Create a a new table option
```scala
val kuduTableOptions = new CreateTableOption()
```

### **Step 8**: Set the table options
```scala
kuduTableOptions.setRangePartitionColumns(List("name").asJava).setNumReplicas(3)
```
Make sure the `scala.collection.JavaConverters._` has been imported (listed in Step 1)

### **Step 9**: Create the table
```scala
kuduContext.createTable(kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)
```