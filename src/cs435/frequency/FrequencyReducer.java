package cs435.frequency;

import java.io.IOException;

import cs435.customObjects.articleUnigram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<articleUnigram,Text, articleUnigram,IntWritable>{
    public void reduce(articleUnigram key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(Text val: values) {
            count++;
        }

        context.write(key, new IntWritable(count));
    }
}