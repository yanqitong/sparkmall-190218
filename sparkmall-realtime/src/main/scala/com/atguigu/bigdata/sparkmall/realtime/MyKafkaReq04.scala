package com.atguigu.bigdata.sparkmall.realtime

import java.util

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object MyKafkaReq04 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("MyKafkaReq04").setMaster("local[*]")

        val streamingContext = new StreamingContext(conf, Seconds(5))

        streamingContext.sparkContext.setCheckpointDir("cp")

        val topic = "ads_log_190218"

        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

        val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaStream.map(data => {
            val datas: Array[String] = data.value().split(" ")
            AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })
        //        adsClickDStream.foreachRDD(rdd=>{
        //            rdd.foreach(println)
        //        })
        //TODO 对数据进行筛选过滤，黑名单里面的userid不要

        //使用广播变量



        //问题1： 会发生空指针异常，因为序列化的规则,所以使用广播变量
        /*
        val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.filter(message => {
            !useridBroad.value.contains(message.userid)
        })
        */
        //问题2 ： 黑名单数据无法更新，应该周期性的获取最新黑名单数据
        val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.transform(rdd => {
            val client: Jedis = MyRedisUtil.getJedisClient
            val userIds: util.Set[String] = client.smembers("blacklist")
            val useridBroad: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userIds)
            client.close()
            rdd.filter(message => {
                !useridBroad.value.contains(message.userid)
            })
        })


        //TODO 把数据转换结构（date-ads-user,1）
        val dateAdsUserToOneDStream: DStream[(String, Long)] = adsClickDStream.map(message => {
            val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.adid + "_" + message.userid, 1L)
        })

        //TODO 对数据进行有状态聚合
        val stateDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
            (seq: Seq[Long], buffer: Option[Long]) => {
                val sum = buffer.getOrElse(0L) + seq.size
                Option(sum)
            }
        }
        stateDStream.foreachRDD(rdd=>{
            rdd.foreach{
                case (k,sum) => {
                    if (sum >= 100){
                        val keys: Array[String] = k.split("_")
                        val userId = keys(2)

                        val jedisClient: Jedis = MyRedisUtil.getJedisClient
                        jedisClient.sadd("blacklist",userId)
                        jedisClient.close()
                    }
                }
            }
        })

        streamingContext.start()
        streamingContext.awaitTermination()

    }
}

case class AdsClickKafkaMessage(timestamp: String, area: String, city: String, userid: String, adid: String)