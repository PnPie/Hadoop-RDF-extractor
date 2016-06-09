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

/**
 * 使用内部类的精髓: 每个内部类可以独立的继承一个类
 */
public class TripleCount {
  /*
   * Input:  text file
   *         InputSplit: n-triple (subjcet, predicate, object)
   *
   * Output: (word, number) pair
   *         number == zero: subject
   *         number == one : predicate
   *         number == tow : object
   *
   * 为了简化接口,MapReduce要求文件输入也得是Key/Value形式
   *
   */
  public static class Map extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable zero = new IntWritable(0);
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable two = new IntWritable(2);
    private Text word = new Text();

    /**
     * 一个Mapper处理一个InputSplit
     *
     * @param key     行
     * @param value   行内容
     * @param context 通过context读取键值对,并将结果(键值对)写在磁盘上
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if (value.getLength() > 0) {
        StringTokenizer itr = new StringTokenizer(value.toString());
        for (int i = 0; i < 3; i++) {
          if (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            switch (i) {
              case 0:
                context.write(word, zero);
                break;
              case 1:
                context.write(word, one);
                break;
              case 2:
                context.write(word, two);
                break;
            }
          } else {
            return;
          }
        }
      }
    }
  }

  /*
   * Input:  (word, number) pair
   * Output: word, number of appearances in subject, in predicate, in object
   *
   */
  public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int subjectCnt = 0;
      int PredicateCnt = 0;
      int ObjectCnt = 0;
      for (IntWritable val : values) {
        switch (val.get()) { //get(): get the Int value of IntWritable variable
          case 0:
            subjectCnt++;
            break;
          case 1:
            PredicateCnt++;
            break;
          case 2:
            ObjectCnt++;
            break;
        }
      }
      context.write(new Text(key + ": " + subjectCnt + " " + PredicateCnt + " " + ObjectCnt), null);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("need 2 parameters as the input and output folders.");
      System.exit(1);
    }

    Configuration conf = new Configuration();

    //Create a Map/Reduce job
    Job job = new Job(conf, "Triple Count RDF");

    //Set configuration
    job.setJarByClass(TripleCount.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    //Set the map output class
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //Set Input and Output path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path output = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(output))
      fs.delete(output, true);
    FileOutputFormat.setOutputPath(job, output);

    //Execution
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
