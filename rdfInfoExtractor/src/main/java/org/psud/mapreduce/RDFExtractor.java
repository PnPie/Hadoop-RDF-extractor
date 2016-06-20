package org.psud.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Static Nested Class: encapsulate better, a logical grouping
 * The static class is associated with its outer class, and
 * Hadoop initiates its Mapper and Reducer by reflection, so it
 * needs it to be static to know the class while compiling.
 */
public class RDFExtractor {
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

  /**
   * Input:  text file (word, number of appearances in subject, in predicate, in object)
   * InputSplit: getSplits()方法默认按行切分返回
   * Output: (number of appearances in predicate, word) pair
   */
  public static class TopKPredicateMapper
    extends Mapper<Object, Text, IntWritable, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      int predicateColumn = 2; // The second column of InputSplit
      String line = value.toString();
      String[] records = line.split(",");
      context.write(new IntWritable(Integer.parseInt(records[predicateColumn])), new Text(records[0]));
    }
  }

  /**
   * Input:  text file
   * InputSpit: n-triple
   * Output: (subject, predicate) pair
   */
  public static class SubjectPredicateMapper
    extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      int i = 0;
      Text word = new Text();
      Text sujet = new Text();
      Text predicat = new Text();
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        switch (i) {
          case 0:
            sujet.set(word);
            break;
          case 1:
            predicat.set(word);
            break;
          default:
            break;
        }
        i++;
      }

      context.write(sujet, predicat);
    }

  }

  /**
   * Input:  text file
   * InputSplit: subject, number of distinct predicates
   * Output: (number of distinct predicates, subject) pair
   */
  public static class InverseMapper extends
    Mapper<Object, Text, IntWritable, Text> {
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      String[] records = line.split(",");
      context.write(new IntWritable(Integer.parseInt(records[1])), new Text(records[0]));
    }
  }

  /**
   * Input:  (word, number) pair
   * Output: word, number of appearances in subject, in predicate, in object
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

  /**
   * Input:  (number of appearances in predicate, word) pair
   * Output: top-10 predicates
   */
  public static class TopKReducer
    extends Reducer<IntWritable, Text, Text, IntWritable> {
    private final int K = 10;
    private TreeMap<Integer, List<String>> map =
      new TreeMap<Integer, List<String>>(Collections.reverseOrder());

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text value : values) {
        if (map.containsKey(key.get())) {
          map.get(key.get()).add(value.toString());
        } else {
          List<String> list = new ArrayList<String>();
          list.add(value.toString());
          map.put(key.get(), list);
        }
        if (map.size() > K) {
          map.remove(map.lastKey());
        }
      }
    }

    /*
     * Called after the Reduce task
     *
     */
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Iterator<Integer> it = map.keySet().iterator();
      for (int i = 0; i < K; i++) {
        if (it.hasNext()) {
          int cnt = it.next();
          for (String str : map.get(cnt))
            context.write(new Text(str), new IntWritable(cnt));
        } else {
          break;
        }
      }
    }
  }

  /**
   * Input:  (subject, predicate) pair
   * Output: subject, number of distinct predicates
   */
  public static class SubGroupPredicate
    extends Reducer<Text, Text, Text, IntWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      int cnt = 0;
      for (Text val : values)
        cnt++;
      context.write(new Text(key.toString() + "," + cnt), null);
    }
  }
}