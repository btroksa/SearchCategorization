package cs435.frequency;

import java.io.IOException;

import cs435.customObjects.articleUnigram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<articleUnigram,IntWritable, articleUnigram, IntWritable>{
    @Override
    public void reduce(articleUnigram key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable val: values) {
            count += val.get();
        }

        context.write(key, new IntWritable(count));
    }
}