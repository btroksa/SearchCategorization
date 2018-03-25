package cs435.tf;

import java.io.IOException;
import java.util.LinkedList;

import cs435.UpdateCount;
import cs435.customObjects.TFValues;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class tfReducer extends Reducer<Text, TFValues, Text, TFValues>{
    public void reduce(Text key, Iterable<TFValues> values, Context context) throws IOException, InterruptedException {

        LinkedList<TFValues> valuesCopy = new LinkedList<>();

        double maxFrequency = Integer.MIN_VALUE;
        for(TFValues val : values) {
            valuesCopy.offer(new TFValues(val.getUnigram(), 0, val.getFrequency()));
            maxFrequency = Math.max(maxFrequency, val.getFrequency());
        }

        System.out.println(key.toString() + " " +  maxFrequency);

        for(TFValues value : valuesCopy) {
            context.write(key, new TFValues(value.getUnigram(), calcTF(value, maxFrequency), value.getFrequency()));
        }
    }

    public double calcTF(TFValues value, int maxFreq){
        double tf = 0.5 + 0.5*(value.getFrequency()/maxFreq);
        return tf;
    }
}