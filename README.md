# TweeterConnect: Make Random Great Again
This repository is an exercise to create a streaming data pipeline to consume tweets
from the Twitter API. The basic idea is to listen to the tweets produced in streaming
provided for the Twitter API and consumed with **Apache Kafka**. The selection of tweets
is based on a specific endpoint provided by us (tweets that
include a word of our choice).


Then we will perform a serie of transformations that
will convert the tweets (string of characters) into a bunch of
lists of random numbers. The production of random numbers is
based on a sorting algorithm. The original idea was to use this numbers to play the
JackPot, based on the randomness of internet social media.


The streaming pipeline and the transformations is implemented with **Apache Flink** and
The output results are exported to a local `.txt` file.


There is also a sink API to store the data in a local **InfluxDB**, if desired.  

