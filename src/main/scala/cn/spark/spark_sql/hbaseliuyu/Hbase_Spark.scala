package cn.spark.spark_sql.hbaseliuyu

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
 * Created by liuyu on 15-9-1.
 */

object Hbase_Spark {
  def main(args: Array[String]) {
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set("hbase.zookeeper.quorum", "localhost")
    configuration.set("hbase.master", "localhost:60000")
    configuration.addResource("/home/liuyu/Spark-anzhuang/hbase-1.1.0.1/conf/hbase-site.xml")
    configuration.set(TableInputFormat.INPUT_TABLE, "scores")
    import org.apache.hadoop.hbase.client.HBaseAdmin
    val hadmin = new HBaseAdmin(configuration)

    val hrdd = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
    hrdd take 1
    val res = hrdd.take(1)
    res(0)
    res(0)._2
    val rs = res(0)._2
    val kv_array = rs.raw

    for(keyvalue <- kv_array ) println("rowkey:"+ new String(keyvalue.getRow)+ " cf:"+
      new String(keyvalue.getFamily()) + " column:" + new String(keyvalue.getQualifier) +
      " " + "value:"+new String(keyvalue.getValue()))

    hrdd.takeSample(true,1,4)
  }
}
