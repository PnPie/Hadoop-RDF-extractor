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
public class rdfMapReduce {
  /**
   * Input: < ,(subject, predicate, object)>
   * Output: < word, (one|two|three)>
   * <p>
   * Hadoop split-up input files into logical
   * {@link org.apache.hadoop.mapreduce.InputSplit}
   * by calling {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(JobContext)}
   * each of which is assigned to a Mapper
   */
  public static class WordRecognitionMapper
    extends Mapper<Object, Text, Text, IntWritable> {
    private final static int COLUMNS = 3;
    private final static IntWritable zero = new IntWritable(0);
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable two = new IntWritable(2);
    private Text word = new Text();

    /**
     * @param key
     * @param value   an InputSplit (by default a line)
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
   * Input:  < , (subject, predicate, object)>
   * Output: <subject, predicate>
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
   * Input:  < word, number >
   * Output: < number, word >
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
   * Input: < word, (one|two|three)>
   * Output: < (word, number, number, number), >
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
   * Input:  < word, (one|two|three)
   * Output: < number, word >
   */
  public static class TopKPredicateReducer
    extends Reducer<Text, IntWritable, IntWritable, Text> {
    private Map<Text, Integer> map = new HashMap<Text, Integer>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
      for (IntWritable value : values) {
        if (value.get() == 1) {
          if (map.containsKey(key))
            map.put(key, map.get(key) + 1);
          else
            map.put(key, 1);
        }
      }
    }

    protected void cleanup(Context context)
      throws IOException, InterruptedException {
      TreeMap<Integer, Set<Text>> treeMap =
        new TreeMap<Integer, Set<Text>>(Collections.<Integer>reverseOrder());
      for (Map.Entry<Text, Integer> entry : map.entrySet()) {
        if (treeMap.containsKey(entry.getValue())) {
          treeMap.get(entry.getValue()).add(entry.getKey());
        } else {
          Set<Text> set = new HashSet<Text>();
          set.add(entry.getKey());
          treeMap.put(entry.getValue(), set);
        }
      }
      writeMap(context, treeMap);
    }
  }

  /**
   * Input:  < number, word >
   * Output: top-10
   */
  public static class SortReducer
    extends Reducer<IntWritable, Text, Text, IntWritable> {
    private final int K = 10;
    private TreeMap<Integer, Set<Text>> treeMap =
      new TreeMap<Integer, Set<Text>>(Collections.<Integer>reverseOrder());

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text value : values) {
        if (treeMap.containsKey(key.get())) {
          treeMap.get(key.get()).add(value);
        } else {
          Set<Text> set = new HashSet<Text>();
          set.add(value);
          treeMap.put(key.get(), set);
        }
        if (treeMap.size() > K) {
          treeMap.remove(treeMap.lastKey());
        }
      }
    }

    /**
     * Called after the Reduce task
     */
    protected void cleanup(Context context)
      throws IOException, InterruptedException {
      writeMap(context, treeMap);
    }
  }

  /**
   * Input:  <subject, predicate>
   * Output: <subject, COUNT DISTINCT (predicate)>
   */
  public static class CountDistValuesReducer
    extends Reducer<Text, Text, Text, IntWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      int cnt = 0;
      for (Text val : values)
        cnt++;
      context.write(new Text(key.toString() + "," + cnt), null);
    }
  }

  /**
   * Write contents of the map in disk through context
   * @param context
   * @param map
   * @throws IOException
   * @throws InterruptedException
   */
  protected static void writeMap(Reducer.Context context, Map<Integer, Set<Text>> map)
    throws IOException, InterruptedException {
    for (Map.Entry<Integer, Set<Text>> entry : map.entrySet())
      for (Text text : entry.getValue())
        context.write(text, new IntWritable(entry.getKey()));
  }
}