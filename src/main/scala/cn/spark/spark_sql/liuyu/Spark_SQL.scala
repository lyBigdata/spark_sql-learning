package cn.spark.spark_sql.liuyu

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by liuyu on 15-8-31.
 */

case class ApacheAccessLog(ipAddress:String,clientIdentd:String,userId:String,
                           dataTime:String,method:String,endpoint:String,
                           protocol:String,reponseCode:Int,contentSize:Long){}


object Spark_SQL {

  //127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839
  final val PATTERN="""^(\S+) (\S+) (\S+) \[([\w/:]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d+) (\d+)""".r

  def initSaprkContext():SparkContext={
    val sparkconf=new SparkConf().setAppName("Spark_SQL_Learning").setMaster("local[2]")
    val sparkcontext=new SparkContext(sparkconf)
    sparkcontext
  }

  def initSqlContext(sc:SparkContext):SQLContext={
    val sqlContext=new SQLContext(sc)
    sqlContext
  }

  def parseLogLine(log:String):ApacheAccessLog={

    val result=PATTERN.findFirstMatchIn(log)  //return Option[Regex.Math]

    if(result.isEmpty){
      throw new RuntimeException("Cannot parse Log Line: "+log)
    }

    val context=result.get   //get match  result

    val host=context.group(1)
    val userinfo=context.group(2)
    val username=context.group(3)
    val dataTime=context.group(4)
    val method=context.group(5)
    val resource=context.group(6)
    val protocol=context.group(7)
    val status=context.group(8).toInt
    val bytesTotal=context.group(9).toLong

    ApacheAccessLog(host,userinfo,username,dataTime,method,resource,protocol,status,bytesTotal)

  }


  object SecondValueOrdering extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      a._2 compare b._2
    }
  }

  def main (args: Array[String])= {

    val sc=initSaprkContext()
    val sqlContext=initSqlContext(sc)

    import sqlContext.implicits._

    val parseAfterLog=sc.textFile("hdfs://master:9000/ApacheLogs/input_logs").map(parseLogLine(_)).cache()

    parseAfterLog.saveAsTextFile("hdfs://master:9000/ApacheLogs/parseAfter_output_logs")

    val countTotal=parseAfterLog.count()  //total lines after parse

    val sqlDF= parseAfterLog.toDF()

    sqlDF.registerTempTable("ApacheLogTable")

    sqlContext.sql("select count(*) from ApacheLogTable").show()   //page view  (PV)

    sqlContext.sql("select ipAddress count(*) as hits from ApachelogTable order by hits desc").show() //UV

    sqlDF.select(avg("contentSize"),max("contentSize") ,min("contentSize")).show()

    sqlDF.groupBy("reponseCode").count().show()

    sqlDF.groupBy("ipAddress").count.filter('count>1).select("ipAddress").show(100)

    sqlDF.groupBy("endpoint").count.sort('count.desc).show(100)

    parseAfterLog.map(log => (log.endpoint, 1)).reduceByKey(_ + _).top(10)(SecondValueOrdering)
  }

}
