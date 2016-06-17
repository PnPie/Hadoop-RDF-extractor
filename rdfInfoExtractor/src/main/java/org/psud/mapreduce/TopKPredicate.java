package org.psud.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopKPredicate {
  /*
   * Input:  text file (word, number of appearances in subject, in predicate, in object)
   *         InputSplit: getSplits()方法默认按行切分返回
   * Output: (number of appearances in predicate, word) pair
   *
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

  /*
   * Input:  (number of appearances in predicate, word) pair
   * Output: top-10 predicates
   *
   */
  public static class TopKPredicateReducer
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

}
