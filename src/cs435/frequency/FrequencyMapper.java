package cs435.frequency;


import java.io.IOException;
import java.util.StringTokenizer;

import cs435.UpdateCount;
import cs435.customObjects.articleUnigram;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, articleUnigram, IntWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if(!value.toString().isEmpty()) {

            int documentId = 0;
            StringBuilder documentTitle = new StringBuilder();
            //get title of the article
            StringTokenizer title = new StringTokenizer(value.toString().split("<====>")[0]);
            while(title.hasMoreTokens()){
                documentTitle.append(title.nextToken());
            }

            //get id of the article
            StringTokenizer id = new StringTokenizer(value.toString().split("<====>")[1]);
            while(id.hasMoreTokens()){
                String docID = id.nextToken();
                documentId = Integer.parseInt(docID);
            }

            String document = documentTitle.toString();
            //get each unigram of the article
            int sentenceNum = 0;
            String [] information = value.toString().split("<====>");
            if(information.length == 3) {
                StringTokenizer data = new StringTokenizer(information[2]);
                while (data.hasMoreTokens()) {
                    String nextToken = data.nextToken();
                    String gram = nextToken.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                    context.write(new articleUnigram(documentId, document, gram), new IntWritable(1));
                }
            }
            context.getCounter(UpdateCount.CNT).increment(1);
        }
    }
}