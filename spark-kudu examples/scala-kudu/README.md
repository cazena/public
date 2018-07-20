# Scala & Kudu

Adapted from [here](https://blog.cloudera.com/blog/2017/02/up-and-running-with-apache-spark-on-apache-kudu/)

## Part 0: Kerberos

Once you have used `ssh` to get in, use `$ kinit user` to gain permission. You will then be prompted to enter your password. Check using `$klist` to see if it's there

---

## Part 1: Setting Up the Table

### **Step 0:** If table is already setup
If the table is setup and you are in a new spark session, you don't need to finish all of Part 1 to move on. You only need to do steps 1, 2, 3, and 4. Then move on.

### **Step 1:** Import dependencies

Import all the following dependences to create be able to create a table:
```scala
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.kudu.client.CreateTableOptions
```

### **Step 2:** Set up Kudu masters (1 master)

If there is only 1 master, replace the `#` with the IP, and the % should be the port (usually `7051`. If you have more than one master, look at the alternate steps below Step 3.
```scala
val master = "ip-##-###-##-###.port:%%%%"
```
If you are using Cazena, it will look something like this:
```scala
val master = "ip-10-134-55-734.cazena.internal:7051"
```


If there are *more than 1* masters than create multiple variables like below, replacing `#` with a different number for each master
```scala
val master# = "ip-##-###-##-###.port:%%%%"
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
Check if the table already exists, and if so delete it
```scala
if (kuduContext.tableExists(kuduTableName)) {
     kuduContext.deleteTable(kuduTableName)
}
```

### **Step 5**: Create the schema
Enter as many fields as needed. The format is `name, type, nullable?`. Types are SQL types and need to be imported individually. You can find a list of DataTypes [here](https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/types/DataTypes.html). You can import these by running `import org.apache.spark.sql.types.TYPE`
```scala
val kuduTableSchema = StructType(
    StructField("name", StringType, false) ::
    StructField("age", IntegerType, true) ::
    StructField("city", StringType, true) :: Nil)
```
The `Nil` means that the value will be empty when nothing is provided

### **Step 6**: Define the primary key
The primary key matches one of the names in the schema
```scala
val kuduPrimaryKey = Seq("name")
```

### **Step 7**: Create a a new table option
```scala
val kuduTableOptions = new CreateTableOptions()
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

---

## Part 1.5: KuduOptions

This step is required for most of the following parts. Create a kuduOptions
```scala
val kuduOptions: Map[String, String] = Map("kudu.table"-> kuduTableName,"kudu.master" -> master)
```

---

## Part 2: Insert Data

### **Step 1**: Case Class
Create a case class. Varies depending on the Schema created above.
```scala
case class Customer(name:String, age:Int, city:String)
```

### **Step 2**: Create a list
Using the case class made above, create a list of data.
```scala
val customers = Array(Customer("name-1", 30, "city-1"), Customer("name-2", 18, "city-2"))
```

### **Step 3**: Create an [RDD](https://www.tutorialspoint.com/apache_spark/apache_spark_rdd.htm)
Now parallelize the list made above to make an RDD
```scala
val customersRDD = sc.parallelize(customers)
```

### **Step 4**: Convert RDD -> DataFrame
Now convert the RDD from above into a DataFrame.
```scala
val customersDF = spark.createDataFrame(customersRDD)
```

### **Step 5**: Insert data
```scala
kuduContext.insertRows(customersDF, kuduTableName)
```

### **Optional Step 6**: Read the table
To read the table, do the following
```scala
spark.read.options(kuduOptions).kudu.show()
```

## Part 2.5: SQLContext
To move any further, SQLContext must be set up. First import the package:
```scala
import org.apache.spark.sql.SQLContext
```
Next you must set up the object
```scala
val sqlContext = new SQLContext(sc)
```

---

## Part 3: Delete Data

### **Step 1:** Register temporary table
Register the dataframe as a temporary table
```scala
customersDF.registerTempTable("customers")
```

### **Step 2:** Filter Dataframe
Filter and create a new dataframe that only has the keys that will be deleted
```scala
val deleteKeysDF = sqlContext.sql("select name from customer where age >20")
```

### **Step 3:** Delete the rows
Now delete the rows
```scala
kuduContext.deleteRows(deleteKeysDF, kuduTableName)
```

### **Optional Step 4**: Read the table
To read the table, do the following
```scala
sqlContext.read.options(kuduOptions).kudu.show
```

---

## Part 4: Upsert Data

### **Step 1:** Create new dataset
Create a new dataset with all the data that needs to be upserted. In this case, the Customer class from Part 1 is used.
```scala
val newAndChangedCustomers = Array(
    Customer("name-3", 25, "chicago"),
    Customer("name-4", 40, "winnipeg"),
    Customer("jordan", 19, "toronto")
)
```

### **Step 2:** Create an RDD
Parallelize the dataset to make an RDD
```scala
val newAndChangedRDD = sc.parallelize(newAndChangedCustomers)
```

### **Step 3:** RDD -> DataFrame
Convert the RDD from above into a dataframe
```scala
val newAndChangedDF = spark.createDataFrame(NewAndChangedRDD)
```

### **Step 4:** Upsert the Data
Now upsert the data
```scala
kuduContext.upsertRows(newAndChangedDF, kuduTableName)
```

### **Optional Step 5**: Read the table
To read the table, do the following
```scala
sqlContext.read.options(kuduOptions).kudu.show
```

---

## Part 5: Update Data

### **Step 1**: Create the dataset
Create a new dataset with an updated version of the row that is being updated
```scala
val modifiedCustomers = Array(Customer("name-5", 25, "chicago"))
```

### **Step 2:** Create an RDD
Parallelize the dataset to make an RDD
```scala
val modifiedCustomersRDD = sc.parallelize(modifiedCustomers)
```

### **Step 3:** RDD -> DataFrame
Convert the RDD from above into a dataframe
```scala
val modifiedCustomersDF = spark.createDataFrame(modifiedCustomersRDD)
```

### **Step 4:** Update the Rows
Now update the row
```scala
kuduContext.updateRows(modifiedCustomersDF, kuduTableName)
```

### **Optional Step 5**: Read the table
To read the table, do the following
```scala
sqlContext.read.options(kuduOptions).kudu.show
```

## **Part 6**: Alter Kudu Table

**Note**: If the kudu table is already linked to a Impala tabe, then the Impala table will update itself automatically. If your table has not been linked and you would like it so, check part 5.5 above.

### Step 1: Import Dependencies
The following dependencies are required upon the dependencies listed at the top
```scala
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.AlterTableOptions
```

### Step 2: Create a new Kudu Client
To be able to alter the table, you have to use `KuduClientBuilder` to create a new client with the master IP.

When entering the master ip, instead of using the IP in step 1, that may look like this: `ip-10-134-55-734.cazena.internal:7051`, cut out the ip and replace the `-`'s with `.`'s. In the end it should look like this `10.134.55.734`.

```scala
val client = new KuduClient.KuduClientBuilder("master-ip").defaultAdminOperationTimeoutMs(600000).build()
```

### **Step 3:** Create object for defaultVal
One of `AlterTableOption()`'s parameters is defaultVal. To supply this, create an objec:
```scala
val o = 0l;
```

### **Step 4:** Alter the table
Finally, alter the table with the following syntax:
```scala
client.alterTable(kuduTableName, new AlterTableOptions().addColumn("column-name", type, o))
```
This will add a non-nullable column. If you want to add a nullable column, use the following syntax:
```scala
.addNullableColumn("addNullable", Type.INT32)
```
Note that you can add as many in one statement by separating by a period. For example:
```scala
client.alterTable(kuduTableName, new AlterTableOptions()
    .addColumn("column-name", Type.INT64, o)
    .addNullableColumn("addNullable", Type.INT32)
)
```
The types that you can use are listed [here](https://www.cloudera.com/documentation/enterprise/5-14-x/topics/kudu_schema_design.html).
