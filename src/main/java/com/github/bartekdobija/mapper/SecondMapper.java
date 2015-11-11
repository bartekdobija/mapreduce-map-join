package com.github.bartekdobija.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class SecondMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private Text valueOut = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    valueOut.set(value.toString().replaceAll(",","-"));
    context.write(key, valueOut);
  }
}
