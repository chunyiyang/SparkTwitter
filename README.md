# SparkTwitter


# Description:
Develop emerging topic detector from Twitter streaming, and do the sentiment analysis on the related tweets so that you can see the public opinion on this emerging topic. 

I use two windows: WindowCounts_180 (180 seconds) and WindowCounts_60 (60 seconds). In each window, calculate the counts for each hashtags by calling “reduceByKeyAndWindow”.
To find out the recent emerging topic, I calculate the difference (resultRDD) by join these two windows and map the value using (for every hashtag: counts in WindowCounts_60 * 3 - WindowCounts_180). The assumption is that if it is an recent emerging topic, 
Count in past 60 seconds > count in past 180 seconds / 3  


# Input:
In order to grant the twitter access to Spark streaming, need to register application on twitter. Need to pass below information as arguments. 
* consumer key
* consumer secret
* access token secret
* access token

# Output:
In the log file, you can find 
* Hashtags and counts for past 180 seconds.
* Hashtags and counts for past 60 seconds.
* Calculation result and emerging topic.
* Sentimental analysis result for the twitters with emerging topic.



