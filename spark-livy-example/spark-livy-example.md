# Submitting Spark Scala jobs via Livy's REST API

Livy provides a REST API for the remote submission of Spark jobs. For detailed use of Livy rest api endpoints refer to below documentation:
https://livy.incubator.apache.org/docs/latest/rest-api.html

One example use case is documented below which submits scala application using the Livy rest API's /batches endpoint. Here we will use an example JAR, spark-examples_2.11-2.1.0.cloudera2.jar. Prior to submitting the job, we upload the JAR file to an HDFS path accessible by our user.

In order to use Livy rest API on a Cazena system, first get the URL for the Livy rest API from the Cloud Sockets page on the console.

### Grab Livy Rest API URL from the cloud sockets page, it should be something like
http://cloudera-manager.ip1iorfujyohfqss.cazena.internal:8443/gateway/cazena/livy/v1

### Here we are using /batches endpoint, so append this endpoint to above URL.

### Now our final URL to make http post request becomes:
http://cloudera-manager.ip1iorfujyohfqss.cazena.internal:8443/gateway/cazena/livy/v1/batches.

### Make http post request as follows:

```bash
Request type: POST
Header: 'Content-Type: application/json'
Authorization: Basic auth (Use PDC username and password)
URL: http://cloudera-manager.ip1iorfujyohfqss.cazena.internal:8443/gateway/cazena/livy/v1/batches
Data:
{"className":"org.apache.spark.examples.SparkPi", "file": "<hdfs path of example jar file>", "proxyUser":""}
```

Note: Here proxyUser key with null/any value is necessary in the JSON data.

The below curl command can be used to make this request:

```bash
curl -i -k -u <username>:<password> -X POST -H 'Content-Type: application/json' 'http://cloudera-manager.ip1iorfujyohfqss.cazena.internal:8443/gateway/cazena/livy/v1/batches' --data '
{"className":"org.apache.spark.examples.SparkPi", "file": "/user/_support/spark-examples_2.11-2.1.0.cloudera2.jar", "proxyUser":""}
```