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

public class EXE3 {

    /*
     * Input:  text file
     *         InputSpit: n-triple
     * Output: (calss, number) pair
     *         number == zero: subject
     *         number == one:  predicate
     *         number == two:  object
     *
     */
    public static class Map1 extends Mapper<Object, Text, Text, IntWritable> {
        //setup(Context) - map(KEYIN, VALUEIN, Context) - cleanup(Context)
    	
        private final static IntWritable zero = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable two = new IntWritable(2);
        private Text word = new Text();

        //A map function is to deal with a inputsplit (a line here)
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                int i = 0;
                Text sujet = new Text();
                Text predicat = new Text();
                StringBuffer objet_buffer = new StringBuffer();
                StringTokenizer itr = new StringTokenizer(value.toString());

                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    if (word.equals(new Text("."))) {
                        break;
                    } else {
                        switch (i) {
                            case 0:
                                sujet.set(word);
                                break;
                            case 1:
                                predicat.set(word);
                                break;
                            default:
                                objet_buffer.append(word.toString());
                                break;
                        }
                        i++;
                    }
                }

                Text objet = new Text(objet_buffer.toString());

                //If the subject/predicate/object is a class, write in the output of Mapper
                if(sujet.toString().contains("http") || predicat.toString().contains("type") && objet.toString().contains("http"))
                  context.write(sujet, zero);
                if(predicat.toString().contains("http"))
                  context.write(predicat, one);
                if(objet.toString().contains("http"))
                  context.write(objet, two);
            }

    }

    /*
     * Input:  (class, number) pair
     * Output: class, number of appearances
     *
     */
    public static class Reduce1 extends Reducer<Text, IntWritable, Text, Text> {
        //setup(Context) - reduce(KEYIN, Iterable<VALUEIN>, Context) - cleanup(Context)

            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int cnt = 0;
                for (IntWritable val : values)
                  cnt++;
                context.write(new Text(key + "," + cnt), null);
            }
        }

    /*
     * Input:  text file
     *         InputSplit: class, number of appearances
     * Output: (number of appearances, class) pair
     *
     */
    public static class Map2 extends Mapper<Object, Text, IntWritable, Text> {
        //setup(Context) - map(KEYIN, VALUEIN, Context) - cleanup(Context)

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] records = line.split(",");
            context.write(new IntWritable(Integer.parseInt(records[1])), new Text(records[0]));
        }
    }

    /*
     * Input: (number of appearances, class) pair
     * Output: top-10 class with number of appearances
     *
     */
    public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {
        //setup(Context) - reduce(KEYIN, Iterable<VALUEIN>, Context) - cleanup(Context)

        //TreeMap: sorted according to its keys(descending ordering)
        private TreeMap<Integer, List<String>> topk = new TreeMap<Integer, List<String>>(Collections.reverseOrder());

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if(topk.containsKey(key.get())) {
                    //If this frequency already exists in the TreeMap, just add the value(URI) for the key
                    topk.get(key.get()).add(value.toString());
                } else {
                    //If not we add the key-value paire to the TreeMap
                    List<String> list = new ArrayList<String>();
                    list.add(value.toString());
                    topk.put(key.get(), list);
                }

                //Keep only the top 10 frequent
                if(topk.size() > 10) {
                    topk.remove(topk.lastKey());
                }
            }
        }

        /*
         * Called after the reduce function
         *
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Integer> it = topk.keySet().iterator();
            int i = 0;
            boolean quit = false;
            while(it.hasNext()) {
                int cnt = it.next();
                for(String s : topk.get(cnt)){
                    context.write(new Text(s), new IntWritable(cnt));
                    if(i++ >= 10){
                        quit = true;
                        break;
                    }
                }
                if(quit)
                  break;
            }
        }
        
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Need 3 parameters as input fold, intermediate fold and output fold.");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        //Create a Map/Reduce job
        Job job_1 = new Job(conf, "RDF JOB1");

        //Set configuration
        job_1.setJarByClass(EXE3.class);
        job_1.setMapperClass(Map1.class);
        job_1.setReducerClass(Reduce1.class);

        //Set the map output class
        job_1.setMapOutputKeyClass(Text.class);
        job_1.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job_1, new Path(args[0]));
        Path intermediate = new Path(args[1]);
        FileSystem fs = FileSystem.getLocal(conf);
        if(fs.exists(intermediate))
          fs.delete(intermediate, true);
        FileOutputFormat.setOutputPath(job_1, intermediate);

        job_1.waitForCompletion(true);

        Job job_2 = new Job(conf, "RDF JOB2");

        job_2.setJarByClass(EXE3.class);
        job_2.setMapperClass(Map2.class);
        job_2.setReducerClass(Reduce2.class);

        job_2.setMapOutputKeyClass(IntWritable.class);
        job_2.setMapOutputValueClass(Text.class);

        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job_2, new Path(args[1]));
        Path output = new Path(args[2]);
        if(fs.exists(output))
          fs.delete(output, true);
        FileOutputFormat.setOutputPath(job_2, output);

        System.exit(job_2.waitForCompletion(true) ? 0 : 1);
    }
} 
