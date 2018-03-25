package cs435.idf;


import java.io.IOException;
import java.util.StringTokenizer;

import cs435.customObjects.TFValues;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class idfMapper extends Mapper<LongWritable, Text, Text, TFValues>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String [] words = value.toString().split("\\t");

        if (words.length > 2) {
            context.write(new Text(words[0]),
                    new TFValues(words[1], 0, Integer.parseInt(words[2])));
        }
    }
}
