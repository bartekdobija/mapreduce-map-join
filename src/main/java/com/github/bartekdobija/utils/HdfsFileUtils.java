package com.github.bartekdobija.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Bartek Dobija on 20/11/2014.
 */
public class HdfsFileUtils {

  public static List<String> readLines(Configuration config, URI path)
      throws IOException {
    return readLines(config, new Path(path));
  }

  public static List<String> readLines(Configuration config, Path path)
      throws IOException {
    FileSystem fs = FileSystem.get(config);

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(fs.open(path)));

    List<String> result = new ArrayList<>();
    String val;
    while ( (val = reader.readLine()) != null ) {
      result.add(val);
    }
    reader.close();

    return result;
  }
}
