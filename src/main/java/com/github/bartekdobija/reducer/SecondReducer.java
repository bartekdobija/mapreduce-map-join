package com.github.bartekdobija.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class SecondReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

  private static final NullWritable NULL = NullWritable.get();

  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text t : values) context.write(t, NULL);
  }
}
