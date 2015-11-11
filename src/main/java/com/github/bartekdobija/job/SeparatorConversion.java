package com.github.bartekdobija.job;

import com.github.bartekdobija.mapper.SecondMapper;
import com.github.bartekdobija.reducer.SecondReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class SeparatorConversion {

  public static final String JOB_NAME = SeparatorConversion.class.getName();

  public static Job newJob(String[] args) throws IOException {

    Job job = Job.getInstance(new Configuration(), JOB_NAME);

    FileInputFormat.addInputPaths(job, args[2]);
    TextOutputFormat.setOutputPath(job, new Path(args[3]));
    job.setMapperClass(SecondMapper.class);
    job.setReducerClass(SecondReducer.class);

    return job;
  }
}
