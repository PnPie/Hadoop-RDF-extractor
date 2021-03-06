package org.psud.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TripleCountJob {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("args number error");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    //Create a WordRecognitionMapper/TripleCountReducer job
    Job job = Job.getInstance(conf, "Triple Count");

    //Set configuration
    job.setJarByClass(org.psud.mapreduce.TripleCountJob.class);
    job.setMapperClass(rdfMapReduce.WordRecognitionMapper.class);
    job.setReducerClass(rdfMapReduce.TripleCountReducer.class);

    //Set the map output class
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //Set Input and Output path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path output = new Path(args[1]);
    if (fs.exists(output))
      fs.delete(output, true);
    FileOutputFormat.setOutputPath(job, output);

    //Execution
    System.out.println(job.waitForCompletion(true) ? "Execution succeeded" : "Execution failed");
  }
}
