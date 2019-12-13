package Functions;

import Utils.ReadingDictionariesJsonIDs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SignalTransformation {

    public static class Tweet2Signal implements FlatMapFunction<Tuple2<String, Integer>, String> {

        Map<String, String> dictMapCh;

        public Tweet2Signal(final Map<String, String> dictMapCh) {
            this.dictMapCh = dictMapCh;
        }

        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) throws Exception{
            Set<String> charList = dictMapCh.keySet();
            char[] charArray = value.f0.toCharArray();
            for (char char0 : charArray) {
                String charStr = String.valueOf(char0);
                if (charList.contains(charStr)){
                    out.collect(dictMapCh.get(charStr));
                } else {
                    out.collect("0.0001");
                }
            }
        }
    }

    public static class sig2loto implements FlatMapFunction<String, String> {
        int[] range = IntStream.rangeClosed(1, 50).toArray();
        int[] range2 = IntStream.rangeClosed(1, 10).toArray();
        //List<Integer> rangeList = Arrays.stream(range).boxed().collect(Collectors.toList());
        //List<Integer> rangeList2 = Arrays.stream(range2).boxed().collect(Collectors.toList());
        ArrayList<Integer> lotoList = new ArrayList<>();
        ArrayList<Integer> starList = new ArrayList<>();
        int iBoo = 0;

        public void flatMap(String value, Collector<String> out) throws Exception{

            for (String sigVal : value.split("\n")) {
                int low;
                int low2;
                int high = range.length;
                int high2 = range2.length;
                Boolean cruz;
                Double sigValD = Double.parseDouble(sigVal);

                if (iBoo%2==0){
                    cruz = sigValD > 0.61207;
                }else if (iBoo%3==0){
                    cruz = sigValD > 0.60;
                } else {
                    cruz = sigValD > 0.62;
                }

                //cruz = sigValD > 0.60;
                // dice move looking for number
                if (cruz) {
                    //low = rangeList.size() / 2;
                    low = range.length/2;
                    //rangeList = rangeList.subList(low, high);
                    range = Arrays.copyOfRange(range, low, high);
                    //low2 = rangeList2.size() / 2;
                    low2 = range2.length/2;
                    //rangeList2 = rangeList2.subList(low2, high2);
                    range2 = Arrays.copyOfRange(range2, low2, high2);
                } else {
                    low = 0;
                    //high = rangeList.size()/2;
                    high = range.length/2;
                    //rangeList = rangeList.subList(low, high);
                    range = Arrays.copyOfRange(range, low, high);
                    low2 = 0;
                    //high2 = rangeList2.size()/2;
                    high2 = range2.length/2;
                    //rangeList2 = rangeList2.subList(low2, high2);
                    range2 = Arrays.copyOfRange(range2, low2, high2);
                }
                //out.collect(Arrays.toString(range));

                //Check the state of the arrays and reset them if need it
                if (range.length==1){
                    Integer insertion = range[0];
                    lotoList.add(insertion);
                    range = IntStream.rangeClosed(1, 50).toArray();
                    for (Integer ele :lotoList) {
                        range = ArrayUtils.removeElement(range, ele);
                        //out.collect(Arrays.toString(range)+"pues que webo");
                    }
                    //out.collect(lotoList.toString());
                }
                if (range2.length==1){
                    starList.add(range2[0]);
                    range2 = IntStream.rangeClosed(1, 10).toArray();
                    for (Integer ele :starList) {
                        range2 = ArrayUtils.removeElement(range2, ele);
                    }
                    //out.collect(starList.toString());
                    //range2 = IntStream.rangeClosed(1, 10).toArray();
                }
                //out.collect(starList.toString());
                //out.collect(lotoList.toString());
                if (lotoList.size()==5 && starList.size()>1) {
                    //out.collect(starList.toString()+"el size de la start list");
                    lotoList.add(starList.get(starList.size()-1));
                    lotoList.add(starList.get(starList.size()-2));

                    out.collect(lotoList.toString());
                    //lotoList.clear();
                    //starList.clear();
                    //range = IntStream.rangeClosed(1, 50).toArray();
                    //range2 = IntStream.rangeClosed(1, 10).toArray();
                }
                if (starList.size()>2){
                    starList.clear();
                }
                if (lotoList.size()>4){
                    lotoList.clear();
                }
                iBoo++;
            }

        }
    }

    public int runBinarySearchIteratively(int[] sortedArray, int key, int low, int high) {
        int index = Integer.MAX_VALUE;

        while (low <= high) {
            int mid = (low + high) / 2;
            if (sortedArray[mid] < key) {
                low = mid + 1;
            } else if (sortedArray[mid] > key) {
                high = mid - 1;
            } else if (sortedArray[mid] == key) {
                index = mid;
                break;
            }
        }
        return index;
    }

    public static class Tweet2Signal2 implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>> {

        Map<String, String> dictMapCh;

        public Tweet2Signal2(final Map<String, String> dictMapCh) {
            this.dictMapCh = dictMapCh;
        }

        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Long>> out) throws Exception{
            Set<String> charList = dictMapCh.keySet();
            char[] charArray = value.f0.toCharArray();
            for (char char0 : charArray) {
                String charStr = String.valueOf(char0);
                Date date= new Date();
                long time = date.getTime();
                Tuple2<String, Long> tuple = new Tuple2<>();
                 if (charList.contains(charStr)){
                    tuple.f0 = dictMapCh.get(charStr);
                    tuple.f1 = time;
                    out.collect(tuple);
                } else {
                    tuple.f0 = "0.0001";
                    tuple.f1 = time;
                    out.collect(tuple);
                }
            }
        }
    }

    public static class Tweet2Signal3 implements FlatMapFunction<Tuple2<String, Integer>, Tuple3<String, String, Long>> {

        Map<String, String> dictMapCh;

        public Tweet2Signal3(final Map<String, String> dictMapCh) {
            this.dictMapCh = dictMapCh;
        }

        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, String, Long>> out) throws Exception{
            Set<String> charList = dictMapCh.keySet();
            char[] charArray = value.f0.toCharArray();
            Date date= new Date();
            long time = date.getTime();
            int timeMachine = 20;
            for (char char0 : charArray) {
                String charStr = String.valueOf(char0);
                Tuple3<String, String, Long> tuple = new Tuple3<>();
                if (charList.contains(charStr)){
                    tuple.f0 = charStr;
                    tuple.f1 = dictMapCh.get(charStr);
                    tuple.f2 = time+timeMachine;
                    out.collect(tuple);
                } else {
                    tuple.f0 = "xx";
                    tuple.f1 = "0.0001";
                    tuple.f2 = time+timeMachine;
                    out.collect(tuple);
                }
                timeMachine+=20;
            }
        }
    }
}
