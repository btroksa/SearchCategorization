package cs435.sentenceTFIDF;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import cs435.customObjects.topNWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

public class sentReducer extends Reducer<Text,Text, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        LinkedList<String> sentences = new LinkedList<>();
        HashMap<String, Double> tfidfVals = new HashMap<>();
        for(Text val: values) {
            if(!val.toString().contains("\t")) {
                sentences.offer(val.toString());
            }
            else{
                String [] parts = val.toString().split("\\t");
                tfidfVals.put(parts[0], Double.parseDouble(parts[1]));
            }
        }

        HashMap<Double, String> sentenceTFIDF = new HashMap<>();
        topNWords top3 = new topNWords();

        for(String sentence : sentences){
            topNWords top = new topNWords();
            for(String word : sentence.split(" ")){
                top.add(tfidfVals.get(word));
            }
            double sentTFIDF = top.getSentencetfidf(5);
            top3.add(sentTFIDF);
            sentenceTFIDF.put(sentTFIDF, sentence);
        }

        StringBuilder threeSentences = new StringBuilder();
        for(int i = 1; i <= 3; i++) {
            double value = top3.getTopK(i);
            if(value != 0){
                threeSentences.append(sentenceTFIDF.get(value)+". ");
            }
        }
        context.write(key, new Text(threeSentences.toString()));

    }
}