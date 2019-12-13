package Utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for reading dictionary files and mapping them into tuples for future analysis
 *
 */
public class ReadingDictionariesJsonIDs {

    public static ArrayList<Tuple2<String, String>> MapListTuple(File textFile) throws IOException {

        BufferedReader brChID = new BufferedReader(new FileReader(textFile));
        ArrayList<Tuple2<String, String>> dictTupleList= new ArrayList<>();
        String st;
        while ((st = brChID.readLine()) != null) {
            Tuple2<String,String> vino = new Tuple2<>();
            ArrayList<String> itemList = new ArrayList<>();
            if (st.split("\t").length==2){
                Boolean cole = Collections.addAll(itemList, st.split("\t"));
                vino.f0 = itemList.get(0);
                vino.f1 = itemList.get(1);
                dictTupleList.add(vino);
            }else {
                vino.f0 = st;
                vino.f1 = st;
                dictTupleList.add(vino);
            }
        }
        return dictTupleList;
    }

    public static Map<String, String> dictMaped(ArrayList<Tuple2<String,String>> tupleList) {
        final Map<String, String> dictMap = new HashMap<>();
        for (Tuple2<String, String> rowDict : tupleList){
            dictMap.put(rowDict.f0, rowDict.f1);
        }
        return dictMap;
    }
}
