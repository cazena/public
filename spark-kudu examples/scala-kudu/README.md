# Scala & Kudu

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
import org.apache.spa rk.sql.types.StringType
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
Check if the table already exsists, and if so delete it
```scala
if (kuduContext.tableExists(kuduTableName)) {
     kuduContext.deleteTable(kuduTableName)
}
```

### **Step 5**: Create the schema
Enter as many fields as needed. Format goes `name, type, nullable?`. Types are SQL types and need to be imported induvidually. You can find a list of DataTypes [here](https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/types/DataTypes.html). You can import these by running `import org.apache.spark.sql.types.TYPE`
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
kuduContext.insertRows(df, kuduTableName)
```

### **Optional Step 6**:
To read the table, do the following
```scala
spark.read.options(kuduOptions).kudu.show()
```





