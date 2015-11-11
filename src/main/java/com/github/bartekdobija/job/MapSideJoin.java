package com.github.bartekdobija.job;

import com.github.bartekdobija.mapper.FirstMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class MapSideJoin {

  public static final String JOB_NAME = MapSideJoin.class.getName();

  public static Job newJob(Configuration config, String[] args)
      throws IOException {

    Job job = Job.getInstance(config, JOB_NAME);
    job.setJarByClass(MapSideJoin.class);
    job.addCacheFile(URI.create(args[0]));

    FileInputFormat.addInputPaths(job, args[1]);
    job.setMapperClass(FirstMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    TextOutputFormat.setOutputPath(job, new Path(args[2]));

    return job;
  }

  public static Job newJob(String[] args) throws IOException {
    return newJob(new Configuration(), args);
  }
}
