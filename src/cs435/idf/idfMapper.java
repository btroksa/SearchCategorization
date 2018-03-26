package cs435.idf;


import java.io.IOException;

import cs435.customObjects.IDFValues;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class idfMapper extends Mapper<LongWritable, Text, Text, IDFValues>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String [] words = value.toString().split("\\t");

        if (words.length > 2) {
            context.write(new Text(words[1]),
                    new IDFValues(words[0], Double.parseDouble(words[2])));
        }
    }
}
