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
[2,16.375703347848926,182]

//Here is the median calculated as the 50th percentile value
val accuracy = 0.001 //you may have to start the spark shell with more resources for higher accuracy
val prevLenMedian = smallDfWithLength.stat.approxQuantile("prev_len", Array(0.5), accuracy)(0)
prevLenMedian: Double = 13.0

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


