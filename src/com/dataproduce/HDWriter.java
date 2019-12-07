package com.dataproduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDWriter {
  public static void main(String args[]) throws IOException {
      Configuration conf = new Configuration();
//1首先需要一个hdfs的客户端对象
      Producer producer = new Producer();
      conf.set("fs.defaultFS", "hdfs://49.234.62.35:9000");
      System.setProperty("HADOOP_USER_NAME", "root");
      conf.set("dfs.client.use.datanode.hostname", "true");
      FileSystem fs = FileSystem.get(conf);
      FSDataOutputStream fsDataOutputStream = fs.create(new Path("/test/test.txt"));
      producer.produce();
      try {
          fsDataOutputStream.write(producer.Data);
          fsDataOutputStream.close();
      } catch (IOException e) {
          e.printStackTrace();
          System.out.println("写入失败");
      }
      fs.close();
  }
}

