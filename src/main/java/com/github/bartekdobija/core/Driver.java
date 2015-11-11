package com.github.bartekdobija.core;

import com.github.bartekdobija.job.MapSideJoin;
import com.github.bartekdobija.job.SeparatorConversion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class Driver extends Configured implements Tool {

  private static final boolean VERBOSE = true;

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(getConf());
    Path intermediate = new Path(args[2]);
    fs.delete(intermediate, true);
    fs.delete(new Path(args[3]),true);
    if (MapSideJoin.newJob(args).waitForCompletion(VERBOSE) == false) {
      return 1;
    }
    if (SeparatorConversion.newJob(args).waitForCompletion(VERBOSE) == false) {
      return 2;
    }
    fs.delete(intermediate, true);
    return 0;
  }
}
