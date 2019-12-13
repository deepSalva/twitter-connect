package Utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Char2NumberMap {

    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);

        BufferedReader reader = new BufferedReader(new FileReader(params.get("input")));

        BufferedWriter writer = new BufferedWriter(new FileWriter(params.get("output")));

        Double counter = 1.0;

        char[] charArray = reader.readLine().toCharArray();
        List<String> array2List = new ArrayList();
        System.out.println(charArray.length);
        for (char charO : charArray) {
            array2List.add(String.valueOf(charO));
        }
        Map<String, Long> counted = array2List.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        System.out.println(counted);
        Collections.shuffle(array2List);
        for (String char1 : array2List) {
            Double temp = (double)Math.round(counter/116 * 100000d) / 100000d;
            writer.write(char1+"\t"+temp+"\n");
            counter++;
        }
        writer.close();
    }
}
