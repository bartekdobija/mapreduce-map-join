package com.github.bartekdobija.mapper;

import com.github.bartekdobija.utils.HdfsFileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  Text keyOut = new Text();
  private List<String> smallTable;
  private StringBuilder builder = new StringBuilder();
  private String separator;
  private static final NullWritable NULL = NullWritable.get();
  public static final String SEPARATOR_KEY = "separator";


  enum FIRST_MAPPER {
    SETUP_CALLED,
    MAP_CALLED,
    MAP_JOIN
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    context.getCounter(FIRST_MAPPER.SETUP_CALLED).increment(1);
    smallTable = HdfsFileUtils.readLines(
        context.getConfiguration(), context.getCacheFiles()[0]);
    separator = context.getConfiguration().get(SEPARATOR_KEY, " ");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    context.getCounter(FIRST_MAPPER.MAP_CALLED).increment(1);

    String bigTableRow = value.toString();
    String[] bigTableColumns = bigTableRow.split(separator);

    for (String row : smallTable) {
      String[] columns = row.split(separator);
      if (columns[0].equals(bigTableColumns[0])) {
        builder.setLength(0);
        builder
            .append(columns[0])
            .append(",")
            .append(columns[1])
            .append(",")
            .append(bigTableColumns[1])
            .append(",")
            .append(columns[2]);

        keyOut.set(builder.toString());
        context.write(keyOut, NULL);
        context.getCounter(FIRST_MAPPER.MAP_JOIN).increment(1);
      }
    }
  }
}
