package cs435.sentenceTFIDF;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class lookUpMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().isEmpty()) {
            String[] words = value.toString().split("\\t");
            if (words.length > 2) {
                context.write(new Text(words[0]),
                        new Text(words[1] + "\t" + Double.parseDouble(words[2])));
            }
        }
    }
}