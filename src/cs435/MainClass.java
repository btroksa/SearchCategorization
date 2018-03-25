package cs435;

import java.io.IOException;

import cs435.customObjects.TFValues;
import cs435.customObjects.articleUnigram;
import cs435.frequency.FrequencyMapper;
import cs435.frequency.FrequencyPartitioner;
import cs435.frequency.FrequencyReducer;
import cs435.tf.tfMapper;
import cs435.tf.tfReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        if (args.length != 3) {
            System.out.printf("Usage: <jar file> <input dir> <intermediate dir> <output dir>\n");
            System.exit(-1);
        }
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(MainClass.class);
        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(FrequencyReducer.class);
        job.setPartitionerClass(FrequencyPartitioner.class);
        job.setNumReduceTasks(6);
        job.setOutputKeyClass(articleUnigram.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        if(success) {
            Job job2 = Job.getInstance(conf);
            job2.setJarByClass(MainClass.class);
            job2.setMapperClass(tfMapper.class);
            job2.setReducerClass(tfReducer.class);
            job2.setNumReduceTasks(6);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(TFValues.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }
}