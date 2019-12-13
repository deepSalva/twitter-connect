package Connector;

import Functions.SignalTransformation;
import Influx.InfluxDBSink;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import data.KeyedDataPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import scala.Serializable;

import java.io.File;
import java.util.*;

import static Utils.ReadingDictionariesJsonIDs.MapListTuple;
import static Utils.ReadingDictionariesJsonIDs.dictMaped;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 *
 * <p>The input is a Tweet stream from a TwitterSource.
 *
 * <p>Usage: <code>Usage: TwitterExample [--output &lt;path&gt;]
 * [--twitter-source.consumerKey &lt;key&gt; --twitter-source.consumerSecret &lt;secret&gt;
 * --twitter-source.token &lt;token&gt; --twitter-source.tokenSecret &lt;tokenSecret&gt;]</code><br>
 *
 * <p>If no parameters are provided, the program is run with default data from
 * {@link TwitterExampleData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 */

public class TwitterConnect {
    //Influx data
    private static final String DATABASE = "Trumpdomizer1HTest";

    public static void main(String[] args)throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterExample [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

        //Upload dictionary to map the tweet chars into numbers
        final File fileMapChar = new File(params.get("input"));
        ArrayList<Tuple2<String, String>> dictChar = MapListTuple(fileMapChar);
        Map<String, String> dictMapCh = dictMaped(dictChar);
        System.out.println(dictMapCh);



        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.getConfig().enableObjectReuse();

        env.setParallelism(params.getInt("parallelism", 1));

        // get input data
        DataStream<String> streamSource;
        if (params.has(TwitterSource.CONSUMER_KEY) &&
                params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) &&
                params.has(TwitterSource.TOKEN_SECRET)
                ) {
            String[] tags = {"Trump"};
            TwitterSource streamSource1 = new TwitterSource(params.getProperties());
            streamSource1.setCustomEndpointInitializer(new FilterEndpoint(tags));
            streamSource = env.addSource(streamSource1);
        } else {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                    "--twitter-source.token <token> " +
                    "--twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
            // get default test text data
            streamSource = env.fromElements(TwitterExampleData.TEXTS);
        }

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .flatMap(new tweetsConsumer())
                // group by words and sum their occurrences
                .keyBy(0).sum(1);

        DataStream<String> stringSignal = tweets
                .flatMap(new SignalTransformation.Tweet2Signal(dictMapCh));

        DataStream<Tuple2<String, Long>> keyPointSignal = tweets
                .flatMap(new SignalTransformation.Tweet2Signal2(dictMapCh));

        DataStream<Tuple3<String, String, Long>> keyPointSignal2 = tweets
                .flatMap(new SignalTransformation.Tweet2Signal3(dictMapCh));

        DataStream<String> keyPointSignal2String = keyPointSignal2
                .flatMap(new Key2String());

        DataStream<String> signal2lotto = tweets
                .flatMap(new SignalTransformation.Tweet2Signal(dictMapCh))
                .flatMap(new SignalTransformation.sig2loto());

/*
        keyPointSignal2.map(new String2KeyedDataPoint2())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSink<>(DATABASE, "20minTest2"));

*/
        signal2lotto.print();
        // emit result
        if (params.has("output")) {
            //tweets.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //tweets.print();
        }

        signal2lotto.writeAsText(params.get("output2"), FileSystem.WriteMode.OVERWRITE);

        // execute program
        env.execute("Twitter Streaming Example");

    }
    /**
     * Deserialize JSON from twitter source
     *
     * <p>Implements a string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static class tweetsConsumer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            String tweetOut = jsonNode.get("text").asText();
            System.out.println(tweetOut);
            out.collect(new Tuple2<>(tweetOut, 1));
        }

    }

    static class FilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

        private final List<String> tags;

        FilterEndpoint(final String... tags) {
            this.tags = Lists.newArrayList(tags);
        }

        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint ep = new StatusesFilterEndpoint();
            ep.trackTerms(tags);
            ep.stallWarnings(false);
            ep.delimited(false);
            return ep;
        }
    }
    private static class String2KeyedDataPoint implements
            MapFunction<Tuple2<String, Long>, KeyedDataPoint<Double>> {

        @Override
        public KeyedDataPoint<Double> map(Tuple2<String, Long> point) throws Exception {
            Double doublePoint = 0.0;
            try{
                doublePoint = Double.parseDouble(point.f0);
            } catch(NumberFormatException ex){ // handle your exception
                System.out.println("Execption handelet: "+point.f0);
            }



            return new KeyedDataPoint<>("Trump", point.f1, doublePoint);
        }
    }

    private static class String2KeyedDataPoint2 implements
            MapFunction<Tuple3<String, String, Long>, KeyedDataPoint<Double>> {

        @Override
        public KeyedDataPoint<Double> map(Tuple3<String, String, Long> point) throws Exception {
            Double doublePoint = 0.0;
            try{
                doublePoint = Double.parseDouble(point.f1);
            } catch(NumberFormatException ex){ // handle your exception
                System.out.println("Execption handelet: "+point.f1);
            }
            return new KeyedDataPoint<>(point.f0, point.f2, doublePoint);
        }
    }

    public static class Key2String implements FlatMapFunction<Tuple3<String, String, Long>, String> {

        @Override
        public void flatMap(Tuple3<String, String, Long> value, Collector<String> out) throws Exception {
            String signalEvent = value.f0 + "\t" + value.f1 + "\t" + value.f2.toString();
            out.collect(signalEvent);
        }

    }

    private static class AsKeyedDataPoint implements FlatMapFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>> {

        @Override
        public void flatMap(KeyedDataPoint<Double> point, Collector<KeyedDataPoint<Double>> collector) throws Exception {
            collector.collect(new KeyedDataPoint<>(point.getKey(), point.getTimeStampMs(), point.getValue()));
        }
    }


}