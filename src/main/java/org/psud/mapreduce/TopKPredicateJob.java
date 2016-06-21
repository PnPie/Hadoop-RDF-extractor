package org.psud.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * an executable class
 */
public class TopKPredicateJob {
  public static void main(String[] args)
    throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("args number error");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Job job = Job.getInstance(conf, "Top K Predicate");
    job.setJarByClass(TopKPredicateJob.class);
    job.setMapperClass(rdfMapReduce.WordRecognitionMapper.class);
    job.setReducerClass(rdfMapReduce.TopKPredicateReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path inter = new Path(args[1]);
    if (fs.exists(inter))
      fs.delete(inter, true);
    FileOutputFormat.setOutputPath(job, inter);
    System.out.println(job.waitForCompletion(true) ? "Execution succeeded" : "Execution failed");
  }
}
