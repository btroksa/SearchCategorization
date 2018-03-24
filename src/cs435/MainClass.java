package cs435;

import java.io.IOException;
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
        job.setJarByClass(profile2.class);
        job.setMapperClass(profile2Mapper.class);
        job.setReducerClass(profile2Reducer.class);
        job.setPartitionerClass(profile2Partitioner.class);
        job.setNumReduceTasks(6);
        job.setOutputKeyClass(MyComparator.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        if(success) {
            Job job2 = Job.getInstance(conf);
            job2.setJarByClass(profile2.class);
            job2.setMapperClass(profile2Mapper2.class);
            job2.setReducerClass(profile2Reducer2.class);
            job2.setPartitionerClass(profile2ReverseOrderPartitioner.class);
            job2.setNumReduceTasks(6);
            job2.setOutputKeyClass(profile2Object.class);
            job2.setOutputValueClass(MyComparator.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }
}