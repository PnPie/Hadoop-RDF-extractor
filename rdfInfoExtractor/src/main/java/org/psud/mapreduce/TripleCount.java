package org.psud.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Static Nested Class: encapsulate better, a logical grouping
 * The static class is associated with its outer class, and
 * Hadoop initiates its Mapper and Reducer by reflection, so it
 * needs it to be static to know the class while compiling.
 */
public class TripleCount {
  /**
   * Input:  text file
   * InputSplit: n-triple (subjcet, predicate, object)
   * <p>
   * Output: (word, number) pair
   * number == zero: subject
   * number == one : predicate
   * number == tow : object
   * <p>
   * Hadoop split-up input files into logical
   * {@link org.apache.hadoop.mapreduce.InputSplit}
   * by calling {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(JobContext)}
   * each of which is assigned to a Mapper
   */
  public static class TripleCountMapper
    extends Mapper<Object, Text, Text, IntWritable> {
    private final static int COLUMNS = 3;
    private final static IntWritable zero = new IntWritable(0);
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable two = new IntWritable(2);
    private Text word = new Text();

    /**
     * @param key
     * @param value   an InputSplit (by default a line):w
     * @param context 通过context读取键值对,并将结果(键值对)写在磁盘上
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      if (value.getLength() > 0) {
        StringTokenizer itr = new StringTokenizer(value.toString());
        for (int i = 0; i < COLUMNS; i++) {
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
  public static class TripleCountReducer
    extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
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
      context.write(new Text(key + "," + subjectCnt + "," + PredicateCnt + "," + ObjectCnt), null);
    }
  }
}
