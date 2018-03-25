package cs435.idf;

import java.io.IOException;
import java.util.LinkedList;

import cs435.UpdateCount;
import cs435.customObjects.TFValues;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class idfReducer extends Reducer<Text, TFValues, Text, TFValues>{
    public void reduce(Text key, Iterable<TFValues> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        long N = context.getCounter(UpdateCount.CNT).getValue();
        LinkedList<TFValues> valuesCopy = new LinkedList<>();

        int maxFrequency = Integer.MIN_VALUE;
        for(TFValues val : values) {
            valuesCopy.offer(val);
            maxFrequency = Math.max(maxFrequency, val.getFrequency());
        }

        for(TFValues val : valuesCopy) {
            context.write(key, new TFValues(val.getUnigram(), calcTF(val, maxFrequency), val.getFrequency()));
        }
    }

    public double calcTF(TFValues value, int maxFreq){
        double tf = 0.5 + 0.5*(value.getFrequency()/maxFreq);
        return tf;
    }
}