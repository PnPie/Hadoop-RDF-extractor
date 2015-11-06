package org.myrdf;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EXE1 {
    /*
     * Input:  text file
     *         InputSplit: n-triple (subjcet, predicate, object)
     * Output: (word, number) pair
     *         number == zero: subject
     *         number == one : predicate
     *         number == tow : object
     *
     */
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        //setup(Context) - map(KEYIN, VALUEIN, Context) - cleanup(Context)

        private final static IntWritable zero = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable two = new IntWritable(2);
        private Text word = new Text();

        //A map function is to deal with a InputSplit (a line)
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int i = 0;
            StringBuffer objet = new StringBuffer();
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (word.equals(new Text("."))) {
                    break;
                } else {
                    switch (i) {
                        case 0:
                            //write subject
                            context.write(word, zero);
                            break;
                        case 1:
                            //write predicate
                            context.write(word, one);
                            break;
                        default:
                            objet.append(word.toString());
                            break;
                    }
                    i++;
                }
            }

            //write object
            context.write(new Text(objet.toString()), two);
        }

    }

    /*
     * Input:  (word, number) pair
     * Output: word, number of appearances in subject, in predicate, in object
     *
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        //setup(Context) - reduce(KEYIN, Iterable<VALUEIN>, Context) - cleanup(Context)

        //Iterable<IntWritable> values: an interface(collector)
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int s_cnt = 0;
            int p_cnt = 0;
            int o_cnt = 0;
            for (IntWritable val : values) {
                switch (val.get()) { //get(): get the Int value of IntWritable variable
                    case 0:
                        s_cnt++;
                        break;
                    case 1:
                        p_cnt++;
                        break;
                    case 2:
                        o_cnt++;
                        break;
                }
            }
            context.write(new Text(key + ": " + s_cnt + " fois(sujet), " + p_cnt + " fois(propriete), " + o_cnt + " fois(objet)"), null);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Need 2 parameters as the input fold and the output fold.");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        //Create a Map/Reduce job
        Job job = new Job(conf, "RDF EXE1");

        //Set configuration
        job.setJarByClass(EXE1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //Set the map output class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Set Input and Output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.getLocal(conf);
        if(fs.exists(output))
          fs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        //Execution
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
