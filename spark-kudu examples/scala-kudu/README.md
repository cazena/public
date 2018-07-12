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

### **Step 2:** Set up Kudu masters

If there is only 1 master, replace the `#` with the IP
```scala
val master = "ip-##-###-##-###.cazena.internal:7051"
```

If there are *more than 1* masters than create multiple variables like below, replacing `#` with a different number for each master
```scala
val master# = "ip-##-###-##-###.cazena.internal:7051"
```

### **Step 2.5**: Skip this step if you only have 1 master

Include all of your masters in the `Seq(master1, master2,...`
```scala
val kuduMasters = Seq(master1, master2, ...).mkString(",")