
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object DemoStateful {
  def main(args: Array[String]) {
    // Parse input argument for accessing twitter streaming api
    if (args.length < 4) {
      System.err.println("Usage: Demo <consumer key> <consumer secret> " + "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("SparkTwitter App").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(60))

    ssc.checkpoint(".")

    // creating twitter input stream, only take english tweets
    val tweets = TwitterUtils.createStream(ssc, None, filters).filter(_.getLang()=="en")
    // print the original tweets
    tweets.print()

    tweets.foreachRDD{(rdd, time) =>
      rdd.map(t => {
        Map(
          "user"-> t.getUser.getScreenName,
          "location" -> Option(t.getGeoLocation).map(geo =>
          { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> t.getText,
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString
        )
      }) .repartition(1).saveAsTextFile("output")
    }

    var topTag = ""
    // retrieve all tweets hash tags
    val hashTags = tweets.flatMap(status => status.getText.split(" ")
      .filter(_.startsWith("#")).map(_.toLowerCase))

    // get hashtag counts in past 180 seconds
    val WindowCounts_180 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(180))

    // get hashtag counts in past 60 seconds
    val WindowCounts_60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))

    // Join these two windows and calculate the difference
    val joinRDD = WindowCounts_60.join(WindowCounts_180)

    val resultRDD = joinRDD.mapValues(x => x._1*3 - x._2)


    // print out the tags and counts in past 180 seconds
    val topWindowCounts_180 = WindowCounts_180.map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // print out total
    topWindowCounts_180.foreachRDD(rdd => {
      val topList = rdd.take(5)
      scala.tools.nsc.io.File("output/result.csv").appendAll(("\n***** Popular " +
        "topics in last 180 seconds (%s total):*****\n").format(rdd.count()))
      topList.foreach{case (count, tag) =>
        scala.tools.nsc.io.File("output/result.csv").appendAll(("Hashtag %s (%s " +
          "tweets) \n").format(tag, count))
      }
    })

    // print out the tags and counts in past 60 seconds
    val topWindowCounts_60 = WindowCounts_60.map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // print out total
    topWindowCounts_60.foreachRDD(rdd => {
      val topList = rdd.take(5)
      scala.tools.nsc.io.File("output/result.csv").appendAll(("\n***** Popular " +
        "topics in last 60 seconds (%s total):*****\n").format(rdd.count()))

      topList.foreach{case (count, tag) =>
        scala.tools.nsc.io.File("output/result.csv").appendAll(("Hashtag %s (%s " +
          "tweets)\n").format(tag, count))
      }
    })


    val topStateCounts = resultRDD.map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    topStateCounts.foreachRDD(rdd => {
      val topList = rdd.take(1)
      topList.foreach{case (count, tag) =>
        topTag = tag
        scala.tools.nsc.io.File("output/result.csv").appendAll(("***** TwitterText " +
          "State %s (window_60 * 3 - window_180) = %s tweets)\n").format(tag, count))
        scala.tools.nsc.io.File("output/result.csv").appendAll(("\n***** Imerging " +
          "TwitterText topic = %s*****\n\n").format(topTag))
        }
    })

    val filter_tweets = tweets.filter {t =>
      val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      tags.contains(topTag)
    }

    val data = filter_tweets.map { status =>
      val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)
      val tags = status.getHashtagEntities.map(_.getText)

      val printText = "Analysis(Test: %s  --> %s --> %s):\n"
        .format(topTag, status.getText,sentiment.toString)
//      println(printText)
      scala.tools.nsc.io.File("output/result.csv").appendAll(printText)

      (status.getText, sentiment.toString, tags)
    }

    data.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      val sqlDataFrame = rdd.toDF("TwitterText", "Sentiment", "HashTag")

      sqlDataFrame.registerTempTable("sentiments")

      sqlContext.sql("select * from sentiments").show(20, false)

    }

    // Start streaming
    ssc.start()
    // Wait for Termination
    ssc.awaitTermination()
  }
}
