package com.dataproduce;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class CAWriter extends Thread {
    public static Cluster cluster;
    public static Session session;
    public static final int repeatTimes = 256;
    public int startPostion;
    public int range;
    public static ArrayList<byte[]> data;
    public static int  count;
    public static CountDownLatch countDownLatch;
    public static void main(String args[]) throws InterruptedException {
        //链接数据库

        cluster = Cluster.builder().addContactPoint("49.234.62.35").withPort(9042).build();
        Session session = cluster.connect("test");
        String cql="DROP TABLE" +" data ";
        session.execute(cql);
         cql="create table data\n" +
                "(\n" +
                "\tid int primary key,\n" +
                "\tdatablock blob\n" +
                ");";
         session.execute(cql);
        //由于Cassandra不支持批量删除，每次只能删表重建。
        System.out.println("请输入线程数量");
        Scanner scanner = new Scanner(System.in);
         count = scanner.nextInt();
        int blockSize = 2014 * 512 / count;//记录下每个线程需要被分配多少整数。
        int lastBlockSize = 2014 * 512 % count;
        Thread[] threads = new Thread[count];
        countDownLatch=new CountDownLatch(count);
        //产生数据
        long start=System.currentTimeMillis();
        Producer producer = new Producer();
        producer.produceData();
        data = (ArrayList<byte[]>) producer.data;
        //给线程分配写入数据
        int curtPosition = 0;
        if (lastBlockSize == 0) {
            for (int i = 0; i < count; i++) {
                CAWriter caWriter = new CAWriter();
                caWriter.startPostion = curtPosition;
                caWriter.range = blockSize;
                curtPosition += blockSize;
                threads[i] = caWriter;
            }
        } else {
            for (int i = 0; i < count - 1; i++) {
                CAWriter caWriter = new CAWriter();
                caWriter.startPostion = curtPosition;
                caWriter.range = blockSize;
                curtPosition += blockSize;
                threads[i] = caWriter;
            }
            CAWriter caWriter = new CAWriter();
            caWriter.startPostion = curtPosition;
            caWriter.range = lastBlockSize;
            threads[count - 1] = caWriter;
        }
        for(int i=0;i<count;i++)
        {
            threads[i].start();
        }
       countDownLatch.await();
        long end=System.currentTimeMillis();
        System.out.println("done");
        System.out.println(end-start);
    }
    @Override
    public void run() {
        ByteBuffer buffer;
        Session session = cluster.connect("test");
       try {
            for (int i = 0; i < range; i++) {
                buffer=ByteBuffer.wrap(data.get(i));
                Insert insert = QueryBuilder.insertInto("test","data").
                        value("id",i).
                        value("datablock",buffer);
                session.execute(insert);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("插入失败");
       }
        countDownLatch.countDown();
        System.out.println("finish");
    }
}