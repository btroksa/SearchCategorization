package cs435.tf;

import java.io.IOException;

import cs435.customObjects.IDFValues;
import cs435.customObjects.TFValues;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class tfMapper extends Mapper<LongWritable, Text, Text, TFValues>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String [] words = value.toString().split("\\t");

        if (words.length > 2) {
            context.write(new Text(words[0]),
                    new TFValues(words[1], 0, Integer.parseInt(words[2])));
        }
    }
}