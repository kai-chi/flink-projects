package kaichi.flink.exercises

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.functions._
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
      .filter(new FindEnglishTweets).setParallelism(5)
      .flatMap(new HashtagWeights).setParallelism(3)
      .map(new ToUpperTweet).setParallelism(2)
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce {(v1, v2) => (v1._1, v1._2 + v2._2, "") }

    streamSource.print()

    env.execute("Twitter Processor")
  }

  class HashtagWeights extends FlatMapFunction[String, (String, Integer, String)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Integer, String)]): Unit = {
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      var hashtagsLength: Integer = 0
      var text = ""

      //get the number of hashtags
      if (jsonNode.has("entities") && jsonNode.get("entities").has("hashtags"))
        hashtagsLength = jsonNode.get("entities").get("hashtags").size

      //get the text of the Tweet
      if (jsonNode.has("text"))
        text = jsonNode.get("text").asText()

      //filter out records without time zone
      if (jsonNode.has("user") && jsonNode.get("user").has("time_zone")) {
        jsonNode.get("user").get("time_zone").asText() match {
          case "null" =>
          case timeZone => out.collect(timeZone, hashtagsLength, text)
        }
      }

    }
  }

  class CountHashtags extends ReduceFunction[(String, Integer)] {
    override def reduce(value1: (String, Integer), value2: (String, Integer)): (String, Integer) = (value1._1, value1._2 + value2._2)
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

  class ToUpperTweet extends MapFunction[(String, Integer, String), (String, Integer, String)] {

    override def map(value: (String, Integer, String)): (String, Integer, String) = {
      (value._1, value._2, value._3.toUpperCase)
    }
  }

  class FindEnglishTweets extends FilterFunction[String] {
    lazy val jsonParser = new ObjectMapper()

    override def filter(x: String): Boolean = {
      val jsonNode = jsonParser.readValue(x, classOf[JsonNode])
      if (jsonNode.has("lang")) {
        jsonNode.get("lang").asText().equals("en")
      }
      else
        false
    }
  }

  //  class AggregateTweets extends AggregateFunction[(String, Integer, String), (String, Integer, String), (String, Integer, String)] {
  //
  //    override def add(value: (String, Integer, String), accumulator: (String, Integer, String)): (String, Integer, String) = {
  //      if (value._2 > accumulator._2) {
  //        (value._1, value._2 + accumulator._2, value._3)
  //      }
  //      else {
  //        (value._1, value._2 + accumulator._2, accumulator._3)
  //      }
  //    }
  //
  //    override def createAccumulator(): (String, Integer, String) = ("", 0, "")
  //
  //    override def getResult(accumulator: (String, Integer, String)): (String, Integer, String) = (accumulator._1, accumulator._2, accumulator._3)
  //
  //    override def merge(a: (String, Integer, String), b: (String, Integer, String)): (String, Integer, String) = {
  //      if (a._2 > b._2) {
  //        (a._1, a._2+b._2, a._3)
  //      }
  //      else {
  //        (a._1, a._2+b._2, b._3)
  //      }
  //    }
  //  }

}
