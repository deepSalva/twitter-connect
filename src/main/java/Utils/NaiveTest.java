package Utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;

public class NaiveTest {
    public static void main(String[] args) throws IOException {
        final File fileMapChar = new File("/home/savi01/dfki/TwitterConnect/src/main/resources/charDict.txt");
        ArrayList<Tuple2<String, String>> dictChar = ReadingDictionariesJsonIDs.MapListTuple(fileMapChar);
        Map<String, String> dictMapCh = ReadingDictionariesJsonIDs.dictMaped(dictChar);

        BufferedReader reader = new BufferedReader(new FileReader("/home/savi01/dfki/twitterOutput/alphabet.txt"));

        char[] charArray = reader.readLine().toCharArray();
        //System.out.println(charArray.length);
        for (int i = 1; i<=charArray.length; i++) {
            if (dictMapCh.get(charArray[i-1])!=null) {

            Double charDouble = Double.parseDouble(dictMapCh.get(charArray[i-1]));
            //System.out.println(charArray[i-1]);
            //System.out.println(charDouble);
            }
        }
        int[] range = IntStream.rangeClosed(1, 50).toArray();
        int[] range2 = IntStream.rangeClosed(1, 10).toArray();
        //System.out.println(Arrays.toString(range));
        int mid = range.length/2;
        Boolean cruz;
        ArrayList<Integer> lotoList = new ArrayList<>();
        ArrayList<Integer> starList = new ArrayList<>();
        ArrayList<ArrayList<Integer>> bigList = new ArrayList<>();
        int iStar = 0;
        for (int ii = 0; ii<20; ii++) {
            int[] slice =range;
            int low;
            int high = range.length;
            int i = 0;
            while (slice.length>1){
                if (i%2==0) {
                    low = slice.length / 2;
                    slice = Arrays.copyOfRange(slice, low, high);
                } else {
                    high = slice.length/2;
                    low = 0;
                    slice = Arrays.copyOfRange(slice, low, high);
                }
                //System.out.println(Arrays.toString(slice));
                i++;
                //System.out.println(i);
            }
            lotoList.add(slice[0]);
            //System.out.println(lotoList.size());
            int[] slice2 =range2;
            high = slice2.length;
            while (slice2.length>1){
                if (i%2==0) {
                    low = slice2.length / 2;
                    slice2 = Arrays.copyOfRange(slice2, low, high);
                } else {
                    high = slice2.length/2;
                    low = 0;
                    slice2 = Arrays.copyOfRange(slice2, low, high);
                }
                //System.out.println(Arrays.toString(slice2));
                i++;
                //System.out.println(i);
            }
            starList.add(slice2[0]);
            //System.out.println(starList.toString());
            if (lotoList.size()==5){
                lotoList.add(starList.get(iStar));
                lotoList.add(starList.get(iStar+1));
                bigList.add(lotoList);
                //System.out.println(bigList.toString());
                lotoList.clear();
            }
        }
        iStar++;
        //System.out.println(bigList.toString());
        int[] range3;

        range3 = IntStream.rangeClosed(3, 5).toArray();
        int[] slice3 = Arrays.copyOfRange(range3, 0, 3/2);
        System.out.println(Arrays.toString(range3));

        System.out.println(Arrays.toString(slice3));
    }
}
