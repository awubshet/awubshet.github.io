# Performance Implications of Partitioning in Apache Parquet

Apache Parquet is a columnar data format for the hadoop ecosystem (much like the ORC format). It supports nested data structures.  It has support for different compression and encoding schemes to be applied to different columns. The schema is embedded in the data itself, so it is a self-describing data format. All this features make it efficient to store and enable performant querying of hdfs data as opposed to row oriented schemes like CSV and TSV.

Parquet also supports partitioning of data based on the values of one or more columns. This article looks at the effects of partitioning on query performance. We are using the spark defaults which defaults to the snappy compression algorithm.

In order to see how parquet files are stored in hdfs, let's save a very small data set with and without partitioning.
Start the spark shell
```bash
$SPARK_HOME/bin/spark-shell
```
```scala
//create a small sample dataset
val capitalCities = Map("USA" -> "Washington DC", "Canada" -> "Ottawa", "Mexico" -> "Mexico City")
val capitalCitiesDf = capitalCities.toSeq.toDF("country","city")

//save the DataFrame in hdfs in the parquet format
capitalCitiesDf.write.format("parquet").mode("overwrite").save("hdfs://hadoop_namenode:9000/parquet_test/capitalcities.parquet")

//save it with partitioning
capitalCitiesDf.write.format("parquet").partitionBy("country").mode("overwrite").save("hdfs://hadoop_namenode:9000/parquet_test/capitalcities_p.parquet")
```
Here is the folder structure in hdfs (output cropped and formated for brevity)
```bash
hdfs dfs -ls -R  /parquet_test/capitalcities.parquet
/cxa/parquet_test/capitalcities.parquet/_SUCCESS
/cxa/parquet_test/capitalcities.parquet/part-00000-87439b68-7536-44a2-9eaa-1b40a236163d-c000.snappy.parquet
/cxa/parquet_test/capitalcities.parquet/part-00001-87439b68-7536-44a2-9eaa-1b40a236163d-c000.snappy.parquet
/cxa/parquet_test/capitalcities.parquet/part-00002-87439b68-7536-44a2-9eaa-1b40a236163d-c000.snappy.parquet

hdfs dfs -ls -R  /parquet_test/capitalcities_p.parquet
/parquet_test/capitalcities_p.parquet/_SUCCESS
/parquet_test/capitalcities_p.parquet/country=Canada
  /parquet_test/capitalcities_p.parquet/country=Canada/part-00001-11394952-d93a-43a1-bb85-475b15e2874d.c000.snappy.parquet
/parquet_test/capitalcities_p.parquet/country=Mexico
  /parquet_test/capitalcities_p.parquet/country=Mexico/part-00002-11394952-d93a-43a1-bb85-475b15e2874d.c000.snappy.parquet
/parquet_test/capitalcities_p.parquet/country=USA
  /parquet_test/capitalcities_p.parquet/country=USA/part-00000-11394952-d93a-43a1-bb85-475b15e2874d.c000.snappy.parquet
```
In the unpartitioned parquet file all the data is in one folder, but in the partitioned parquet file the data is in three folders denoting the column upon which the data is partitioned against (country=Canada, country=Mexico and country=USA)

Let's have some data for testing. The wikipedia click stream files (https://dumps.wikimedia.org/other/clickstream/readme.html) are a monthly dataset of counts of referer and page pairs in TSV. Here is the schema:

    prev: the title of of referer URL 
    curr: the title of the article the client requested
    type: describes (prev, curr)
        link: if the referer and request are both articles and the referer links to the request
        external: if the referer host is not en(.m)?.wikipedia.org
        other: if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer.
    n: the number of occurrences of the (referer, resource) pair

Let's download the file for the month of February 2018 and copy it to hdfs.

``` bash
# Download
wget https://dumps.wikimedia.org/other/clickstream/2018-02/clickstream-enwiki-2018-02.tsv.gz
# Unzip
gunzip clickstream-enwiki-2018-02.tsv.gz
# copy to hdfs
hdfs dfs -copyFromLocal clickstream-enwiki-2018-02.tsv /parquet_test/
```

Let's start a spark shell on yarn (disabling the dynamic executor allocation so that our time measurements are against the same amount of resource allocated)
```
spark-shell --master yarn --driver-memory 1g --executor-memory 8g --num-executors 8 --conf spark.default.parallelism=1024 --conf spark.executor.cores=4 --conf spark.dynamicAllocation.enabled=false
```

Read the tsv file
```scala
import org.apache.spark.sql.types._
var clickStreamSchema = StructType(Array(
    StructField("prev", StringType, true),
    StructField("curr", StringType, true),
    StructField("type", StringType, true),
    StructField("count",   IntegerType, true)
    ))
val tsvDf = spark.read.format("csv").option("sep", "\t").schema(clickStreamSchema).load("/cxa/parquet_test1/clickstream-enwiki-2018-02.tsv")
```
Let's artificially make this dataset larger by adding UUID column multiple times and unioning the resulting data frames. 

```scala
import java.util.UUID.randomUUID

//A udf which returns a UUID
val randomUUIDUdf = udf(() => randomUUID.toString)

//An empty DataFrame which is the zero value of our accumulator when performing the unions
val emptyDf = Seq.empty[(String, String, String, Int, String)].toDF("prev", "curr", "type", "count", "uuid")

//50 times as many recored with an extra UUID column
val bigDf = Range(0,50).foldLeft(emptyDf)((a,e) => a.union(tsvDf.withColumn("uuid", randomUUIDUdf())))

//A sanity check if we have 50 times a many records
tsvDf.count() = 25796624
bigDf.count() = 1289831200
```

Now let's save them in the parquet format. Save `bigDf` in the tsv format too.
```scala
//Save them in the parquet format
bigDf.write.format("parquet").mode("overwrite").save("hdfs://hadoop_namenode:9000/parquet_test/mydata.parquet")
tsvDf.write.format("parquet").mode("overwrite").save("hdfs://hadoop_namenode:9000/parquet_test/clickstream-enwiki-2018-02.parquet")
//Save bigDf in tsv
bigDf.write.format("csv").option("sep", "\t").mode("overwrite").save("hdfs://hadoop_namenode:9000/parquet_test/mydata.csv")
```

Let's check the sizes of the files (folders) in hdfs
```bash
/usr/local/hadoop/bin/hdfs dfs -du -s -h /parquet_test/*
414.3 M  /parquet_test/clickstream-enwiki-2018-02.parquet
1.1 G    /parquet_test/clickstream-enwiki-2018-02.tsv
100.4 G  /parquet_test/mydata.csv
63.5 G   /parquet_test/mydata.parquet
```
This is a small dataset, but we can see that the parquet format needed about 62% less disk space the smaller dataset, and 37% for the larger data sense (the larger dataset has a UUID column which can't be as effectively encoded and compressed as a column with possible repetitions). 

We can partition the data on any one or more of the columns. Since we don't know the contents or our data set, let's append a new column with data distribution we control. This column will will have the value "GGOUP1" or "GROUP2", based on whether the length of the 'prev' column is greater than or less than the median length of that column.

```scala
//Read the parquet files
val myDataDf = spark.read.format("parquet").load("hdfs:///parquet_test/mydata.parquet")
//load the smaller data set so that we can do the median calculation faster on it
val smallDf = spark.read.format("parquet").load("hdfs:///parquet_test/clickstream-enwiki-2018-02.parquet")

//A udf which calculates string length
val strLenUdf = udf((v: String) => v.length)

//Let's add the length column to the dataframe
val smallDfWithLength = smallDf.withColumn("prev_len", strLenUdf(col("prev")))

//Let' see the minimum, average and max values of this 'prev_len' column
smallDfWithLength.agg(min(col("prev_len")), avg(col("prev_len")), max(col("prev_len"))).head
//[2,16.375703347848926,182]

//Here is the median calculated as the 50th percentile value
val accuracy = 0.001 //you may have to start the spark shell with more resources for higher accuracy
val prevLenMedian = smallDfWithLength.stat.approxQuantile("prev_len", Array(0.5), accuracy)(0)
//prevLenMedian: Double = 13.0

//add a length column to our data set
val myDataDfWithLength = myDataDf.withColumn("prev_len", strLenUdf(col("prev")))

//A udf which returns "GROUP1" or "GROUP2" based on string length
val prevLenCatUdf = udf((l: Int) => if (l < prevLenMedian) "GROUP1" else "GROUP2")

//Append the column 'cat' with the possible values of "GROUP1" or "GROUP2"
val myData2Df = myDataDfWithLength.withColumn("cat", prevLenCatUdf(col("prev_len")))
```

Hopefully we now have data distributed evenly against our two groups. Let's check that with simple counts
```scala
myData2Df.filter("cat = 'GROUP1'").count() = 628896300 / 48.8%
myData2Df.filter("cat = 'GROUP2'").count() = 660934900 / 51.2%
```
Let's now save the dataframe with the extra columns without and with partitoning
```scala
//no partitions
myData2Df.write.format("parquet").mode("overwrite").save("hdfs:///parquet_test/mydata2.parquet")
//partitioned against the 'cat' column
myData2Df.write.format("parquet").partitionBy("cat").mode("overwrite").save("hdfs:///parquet_test/mydata2_p.parquet")
```

Now let's read these two parquet files and compare query times. For each of the time measurements below, the spark shell is restarted so that there is no caching.
```scala
val myData2 = spark.read.format("parquet").load("hdfs://hb01rm01-np.pod06.wdc01.cxa.ibmcloud.com:9000/cxa/parquet_test/mydata2.parquet")
// 2.455856714 s time needed to setup the meta data
myData2.count()
// 6.211262667 s
myData2.filter("cat = 'GROUP1'").count()
// 9.107866222 s
myData2.filter("prev_len > 13").count()
// 9.745776 s
myData2.filter("type = 'external'").count()
// 13.18347967 s

val myData2_p = spark.read.format("parquet").load("hdfs://hb01rm01-np.pod06.wdc01.cxa.ibmcloud.com:9000/cxa/parquet_test/mydata2_p.parquet")
// 2.540029429 s
myData2_p.count()
// 8.496096333 s
myData2_p.filter("cat = 'GROUP1'").count()
// 6.785112778 s
myData2_p.filter("prev_len > 13").count()
// 10.262274 s

myData2_p.filter("type = 'external'").count()
// 18.42553933 s
```

Now let's us arrange the data into 5 groups instead of 2. We do that by calculating the quitiles instead of the 50% percentile.
```scala
//Load the data sets
val myData2 = spark.read.format("parquet").load("hdfs:///parquet_test/mydata2.parquet")
val smallDf = spark.read.format("parquet").load("hdfs:///parquet_test/clickstream-enwiki-2018-02.parquet")

val strLenUdf = udf((v: String) => v.length)
val smallDfWithLength = smallDf.withColumn("prev_len", strLenUdf(col("prev")))

val accuracy = 0.001
val quintiles = Range(2,10,2).map(_/10.0).toArray
// Array(0.2, 0.4, 0.6, 0.8)
// do the computation on the smaller data frame as the distribution is the same
val q5 = smallDfWithLength.stat.approxQuantile("prev_len", quintiles, accuracy)
// Array(11.0, 12.0, 14.0, 22.0)

// get the quintile boudaries in separate variables
val (q1, q2, q3, q4) = (q5(0), q5(1), q5(2), q5(3))

// A udf that assigns groups 1 to 5 based on string length
val prevLenCat5 = udf((l: Int) => if (l < q1) "GROUP1" else if (l < q2) "GROUP2" else if (l < q3) "GROUP3" else if (l < q4) "GROUP4" else "GROUP5" )

// Replace the exiting 'cat' column with the new 5 groups
val myData5 = myData2.drop("cat").withColumn("cat", prevLenCat5(col("prev_len")))

// check the data distribution
myData5.filter("cat = 'GROUP1'").count() = 158197000 / 12.3%
myData5.filter("cat = 'GROUP2'").count() = 258537450 / 20.0%
myData5.filter("cat = 'GROUP3'").count() = 257951950 / 20.0%
myData5.filter("cat = 'GROUP4'").count() = 356694700 / 27.7%
myData5.filter("cat = 'GROUP5'").count() = 258450100 / 20.0%
```
Not evenly distributed like the grouping into two, but good enough for our measurements.

Now, let's save our quintiled data 
```scala
// No partitioning
myData5.write.format("parquet").mode("overwrite").save("hdfs://parquet_test/mydata5.parquet")
// partitioned
myData5.write.format("parquet").partitionBy("cat").mode("overwrite").save("hdfs:///parquet_test/mydata5_p.parquet")
```

Let's reload the data and do the same measures as above
```scala
val myData5 = spark.read.format("parquet").load("hdfs:///parquet_test/mydata5.parquet")
// 2.113559923
myData5.count()
// 5.781052333 s
myData5.filter("cat = 'GROUP2'").count()
// 10.357212 s
myData5.filter("prev_len > 13").count()
// 7.238561 s
myData5.filter("type = 'external'").count()
// 11.06506467 s

val myData5_p = spark.read.format("parquet").load("hdfs:///parquet_test/mydata5_p.parquet")
// 2.341337769
myData5_p.count()
// 9.462628667 s
myData5_p.filter("cat = 'GROUP2'").count()
// 4.795245333 s
myData5_p.filter("prev_len > 13").count()
// 8.046922667 s
myData5_p.filter("type = 'external'").count()
// 26.915488 s
```

The above results show that partitioning does help when the partitioning column has only a number of possible values and we query against that column. When we had only two possible vales, the query was 25.5% faster. With the possible five values, it got to be 53.7% faster. Queries against other columns are slightly slower. Let's now see the effect of having a column with a 100 possible values.

```scala
val rand = new scala.util.Random
val randLimit = 1000
val nextRandomIntUdf = udf(() => "GROUP" + rand.nextInt(randLimit))
// load the data and transform
val myData5 = spark.read.format("parquet").load("hdfs:///parquet_test/mydata5.parquet")
val myData1k = myData5.drop("cat").withColumn("cat", nextRandomIntUdf())

// Save the unparitioned and partitioned parquet data to hdfs
myData1k.write.format("parquet").mode("overwrite").save("hdfs:/parquet_test/mydata1k.parquet")
myData1k.write.format("parquet").partitionBy("cat").mode("overwrite").save("hdfs:///parquet_test/mydata1k_p.parquet")
```

Let's do the same measurements for our new data set where the column 'cat' can have one of possible 100 values.
```scala
val myData1k = spark.read.format("parquet").load("hdfs:///parquet_test/mydata1k.parquet")
// 1.929997125 s
myData1k.count()   
// 4.885199667 s
myData1k.filter("cat = 'GROUP357'").count()
// 9.909249 s
myData1k.filter("prev_len > 13").count()
// 6.130633333 s
myData1k.filter("type = 'external'").count()
// 10.44633 s

val myData1k_p = spark.read.format("parquet").load("hdfs:///parquet_test1/mydata1k_p.parquet")
// 9.728571429 s
myData1k_p.count()
// 58.37241867 s
myData1k_p.filter("cat = 'GROUP357'").count()
// 1.650945667 s
myData1k_p.filter("prev_len > 13").count()
// 40.44449867 s
myData1k_p.filter("type = 'external'").count()
// 51.88738233 s
```

