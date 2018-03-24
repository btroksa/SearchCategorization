package cs435.tf;

import java.io.IOException;
import java.util.StringTokenizer;

import cs435.customObjects.articleUnigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class tfMapper extends Mapper<LongWritable, Text, articleUnigram, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        if(!value.toString().isEmpty()) {

            int documentId = 0;
            String documentTitle = "";

            StringTokenizer title = new StringTokenizer(value.toString().split("<====>")[0]);
            while(title.hasMoreTokens()){
                documentTitle += title.nextToken();
            }

            StringTokenizer id = new StringTokenizer(value.toString().split("<====>")[1]);
            while(id.hasMoreTokens()){
                String docID = id.nextToken();
                documentId = Integer.parseInt(docID);
            }

            int sentenceNum = 0;
            StringTokenizer data = new StringTokenizer(value.toString().split("<====>")[2]);
            while (data.hasMoreTokens()) {
                String nextToken = data.nextToken();
                if(nextToken.contains(".")){
                    
                }
                String out = nextToken.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                context.write(new articleUnigram(documentId, documentTitle, out, sentenceNum), new Text("one"));
            }
        }
    }
}