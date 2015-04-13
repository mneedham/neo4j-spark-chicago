import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CrimeApp3 {

  def merge(srcPath: String, dstPath: String, header: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    MyFileUtil.copyMergeWithHeader(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, header)
  }

  def main(args: Array[String]) {
    // https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2
    var crimeFile = "/Users/markneedham/Downloads/Crimes_-_2001_to_present.csv"

    if(args.length >= 1) {
      crimeFile = args(0)
    }
    println("Using %s".format(crimeFile))

    val conf = new SparkConf().setAppName("Chicago Crime Dataset")

    val sc = new SparkContext(conf)
    val crimeData = sc.textFile(crimeFile).cache()
    val withoutHeader: RDD[String] = dropHeader(crimeData)

    val initialFile = sc.broadcast(withoutHeader)

    generateFile("/tmp/primaryTypes.csv", withoutHeader, columns => Array(columns(5).trim(), "CrimeType"), "crimeType:ID(CrimeType),:LABEL")
    generateFile("/tmp/beats.csv", withoutHeader, columns => Array(columns(10), "Beat"), "id:ID(Beat),:LABEL")
    generateFile("/tmp/crimes.csv", withoutHeader, columns => Array(columns(0),"Crime", columns(2), columns(6)), "id:ID(Crime),:LABEL,date,description", false)
    generateFile("/tmp/crimesPrimaryTypes.csv", withoutHeader, columns => Array(columns(0),columns(5).trim(), "CRIME_TYPE"), ":START_ID(Crime),:END_ID(CrimeType),:TYPE")
    generateFile("/tmp/crimesBeats.csv", withoutHeader, columns => Array(columns(0),columns(10), "ON_BEAT"), ":START_ID(Crime),:END_ID(Beat),:TYPE")
  }

  def transformRDD(withoutHeader: RDD[String], fn: Array[String] => Array[String], header: String , fileName:String, separator: String = ",") = {
    print("transforming RDD... ")
    (withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        fn(columns).mkString(separator)
      })
    }).distinct(), header, fileName)
  }

  def generateFile(file: String, withoutHeader: RDD[String], fn: Array[String] => Array[String], header: String , distinct:Boolean = true, separator: String = ",") = {
    FileUtil.fullyDelete(new File(file))

    val tmpFile = "/tmp/" + System.currentTimeMillis() + "-" + file
    val rows: RDD[String] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        fn(columns).mkString(separator)
      })
    })

    if (distinct) rows.distinct() saveAsTextFile tmpFile else rows.saveAsTextFile(tmpFile)

    merge(tmpFile, file, header)
  }

  // http://mail-archives.apache.org/mod_mbox/spark-user/201404.mbox/%3CCAEYYnxYuEaie518ODdn-fR7VvD39d71=CgB_Dxw_4COVXgmYYQ@mail.gmail.com%3E
  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }


}
