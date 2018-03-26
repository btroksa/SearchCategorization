package cs435.idf;

import java.io.IOException;
import java.util.LinkedList;

import cs435.UpdateCount;
import cs435.customObjects.IDFValues;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class idfReducer extends Reducer<Text, IDFValues, Text, IDFValues>{
    public void reduce(Text key, Iterable<IDFValues> values, Context context) throws IOException, InterruptedException {

        double ni = 0;
        long N = context.getConfiguration().getLong(UpdateCount.CNT.name(), 0);
        LinkedList<IDFValues> valuesCopy = new LinkedList<>();

        for(IDFValues val : values) {
            valuesCopy.offer(new IDFValues(val.getDocumentID(), val.getIDFValue()));
            ni++;
        }
        if(key.toString().equals("while")){
            System.out.println(ni);
        }
        for(IDFValues val : valuesCopy) {
            context.write(new Text(val.getDocumentID()), new IDFValues(key.toString(), calcIDF(val, N, ni)));
        }
    }

    public double calcIDF(IDFValues value, long N, double ni){
        double tfidf = value.getIDFValue() * Math.log10(N/ni);
        return tfidf;
    }
}