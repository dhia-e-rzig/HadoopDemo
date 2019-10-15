package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, DoubleWritable>{

    private final static DoubleWritable val = new DoubleWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        while (itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }
        String[] arr = value.toString().split(",");
        word.set(arr[4]);
        val.set(Double.valueOf(arr[5]));
        context.write(word,val);
    }
}