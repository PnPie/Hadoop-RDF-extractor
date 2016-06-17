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
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 3) {
      System.err.println("args number error");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Job job1 = Job.getInstance(conf, "Triple Count");
    job1.setJarByClass(org.psud.mapreduce.TripleCountJob.class);
    job1.setMapperClass(org.psud.mapreduce.TripleCount.TripleCountMapper.class);
    job1.setReducerClass(org.psud.mapreduce.TripleCount.TripleCountReducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    Path inter = new Path(args[1]);
    if (fs.exists(inter))
      fs.delete(inter, true);
    FileOutputFormat.setOutputPath(job1, inter);

    if (job1.waitForCompletion(true)) {
      Job job2 = Job.getInstance(conf, "TopKPredicate");
      job2.setJarByClass(org.psud.mapreduce.TopKPredicateJob.class);
      job2.setMapperClass(org.psud.mapreduce.TopKPredicate.TopKPredicateMapper.class);
      job2.setReducerClass(org.psud.mapreduce.TopKPredicate.TopKPredicateReducer.class);
      job2.setMapOutputKeyClass(IntWritable.class);
      job2.setMapOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job2, new Path(args[1]));
      Path output = new Path(args[2]);
      if (fs.exists(output))
        fs.delete(output, true);
      FileOutputFormat.setOutputPath(job2, output);
      System.out.println(job2.waitForCompletion(true) ? "Execution succeeded" : "Execution failed");
    }
  }
}
