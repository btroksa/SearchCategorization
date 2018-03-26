package cs435.sentenceTFIDF;

import java.io.IOException;
import java.util.StringTokenizer;

import cs435.UpdateCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class datasetMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if(!value.toString().isEmpty()) {

            String documentId = "";

            //get id of the article
            StringTokenizer id = new StringTokenizer(value.toString().split("<====>")[1]);
            while(id.hasMoreTokens()){
                documentId = id.nextToken();
            }

            //get each unigram of the article
            int sentenceNum = 0;
            String [] information = value.toString().split("<====>");
            if(information.length == 3) {
                StringTokenizer data = new StringTokenizer(information[2]);
                StringBuilder sb = new StringBuilder();
                while (data.hasMoreTokens()) {
                    String nextToken = data.nextToken();
                    String gram = nextToken.replaceAll("[^A-Za-z0-9.]", "").toLowerCase();
                    if(gram.contains(".")) {
                        sb.append(gram.replaceAll("[.]", ""));
                        context.write(new Text(documentId), new Text(sb.toString()));
                        sb = new StringBuilder();
                    }
                    else{
                        sb.append(gram + " ");
                    }
                }
            }
        }
    }
}