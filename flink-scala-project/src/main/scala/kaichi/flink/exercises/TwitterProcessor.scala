package kaichi.flink.exercises

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

object TwitterProcessor {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()
    val props = new Properties()

    props.setProperty(TwitterSource.CONSUMER_KEY, config.getString("twitter.consumerKey"))
    props.setProperty(TwitterSource.CONSUMER_SECRET, config.getString("twitter.consumerSecret"))
    props.setProperty(TwitterSource.TOKEN, config.getString("twitter.token"))
    props.setProperty(TwitterSource.TOKEN_SECRET, config.getString("twitter.tokenSecret"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamSource = env
      .addSource(new TwitterSource(props))
      .assignTimestampsAndWatermarks(new EventAssigner)
      .flatMap(new HashtagWeights)
      .keyBy(1)
      .timeWindow(Time.seconds(5))
      .reduce(new CountHashtags)

    streamSource.print()

    env.execute("Twitter Processor")
  }

  class HashtagWeights extends FlatMapFunction[String, (String, Integer)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Integer)]): Unit = {
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      var hashtags: Integer = 0

      //get the number of hashtags
      if (jsonNode.has("entities") && jsonNode.get("entities").has("hashtags"))
        hashtags = jsonNode.get("entities").get("hashtags").size

      //filter out records without time zone
      if (jsonNode.has("user") && jsonNode.get("user").has("time_zone")) {
        jsonNode.get("user").get("time_zone").asText() match {
          case "null" =>
          case s => out.collect(s, hashtags)
        }
      }

    }
  }

  class CountHashtags extends ReduceFunction[(String, Integer)] {
    override def reduce(value1: (String, Integer), value2: (String, Integer)): (String, Integer) =
      return new Tuple2[String, Integer](value1._1, value1._2 + value2._2)
  }

  class EventAssigner extends AssignerWithPeriodicWatermarks[String] {

    lazy val jsonParser = new ObjectMapper()
    var currentMaxTimestamp: Long = 0L

    // TODO: fix it
    override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - 10000)

    override def extractTimestamp(t: String, l: Long): Long = {
      val jsonNode = jsonParser.readValue(t, classOf[JsonNode])
      if (jsonNode.has("created_at")) {
        val createdAt = jsonNode.get("created_at").asText
        val date = LocalDateTime.parse(createdAt, Utils.formatter)
        val timestamp = date.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
      else {
        LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
      }
    }
  }

  object Utils {
    // "Fri Mar 16 14:33:33 +0000 2018"
    val formatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy")
  }

}
