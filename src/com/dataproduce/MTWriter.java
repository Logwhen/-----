package com.dataproduce;

import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class MTWriter extends Thread {
    private byte[] bytes;
    private File file;
    private int blogSize;
    private int startPosition;
    private int signal;
    private byte[] data;
    private static CountDownLatch countDownLatch;
    private static int style;
    private static BufferedOutputStream buf;
    private static DataOutputStream dos;
    //构造函数如下：
    public MTWriter(byte[] data,int blogSize,File file,int startPosition)
    {
        this.file=file;
        this.blogSize=blogSize;
        this.startPosition=startPosition;
        this.data=data;
        this.bytes=new byte[blogSize];
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        int threadNumbers = 0;
        File file=new File("sample.txt");
        System.out.println("请输入线程数量，最大为32");
        Scanner scanner=new Scanner(System.in);
        threadNumbers=scanner.nextInt();
       countDownLatch=new CountDownLatch(threadNumbers);

        System.out.println("请选择写入方式:1.RandomAccessFile 2.BufferedOutPutStream 3.DataOutPutStream");
        style=scanner.nextInt();

        //进行初步的划分运算
        if(threadNumbers>32)threadNumbers=32;
        int blocksize=2014*512*256*4/threadNumbers;
        int LastBlockSize=2014*512*256*4%threadNumbers;
        int startPosition=0;

        //开始执行：
        com.dataproduce.Producer producer=new com.dataproduce.Producer();
        long startTime =  System.currentTimeMillis();
        producer.produce();

        Thread[] threads = new Thread[threadNumbers];
        for(int i=0;i<threadNumbers-1;i++)
        {
            MTWriter mtWriter=new MTWriter(producer.Data,blocksize,file,startPosition);
            mtWriter.InitializeWriting();
            threads[i]=mtWriter;
            startPosition+=blocksize;
        }
        MTWriter mtWriter=new MTWriter(producer.Data,blocksize,file,startPosition);
        mtWriter.InitializeWriting();
        threads[threadNumbers-1]=mtWriter;
        switch (style)
        { case 1:
            for(int i=0;i<threadNumbers;i++)
            {
            threads[i].start();
            }
            countDownLatch.await();
            long endTime = System.currentTimeMillis();
            System.out.println(endTime - startTime);
            System.out.println("done");
            break;
            case 2:
                 buf=new BufferedOutputStream(new FileOutputStream(file));
                for(int i=0;i<threadNumbers;i++)
                {
                    threads[i].run();
                }
                countDownLatch.await();
                endTime = System.currentTimeMillis();
                buf.close();
                System.out.println(endTime - startTime);
                System.out.println("done");
                break;
            case 3:
                dos=new DataOutputStream(new FileOutputStream(file));
                for(int i=0;i<threadNumbers;i++)
                {
                    threads[i].run();
                }
                countDownLatch.await();
                endTime = System.currentTimeMillis();
                dos.close();
                System.out.println(endTime - startTime);
                System.out.println("done");
                break;
        }
    }
    public void InitializeWriting()
    {
        System.arraycopy(this.data,this.startPosition,this.bytes,0,this.blogSize);
    }

    @Override
    public void run()
    {
        switch (style) {
            case 1:
                try {
                    RandomAccessFile raf = new RandomAccessFile(file, "rw");
                    raf.seek((long) startPosition);
                    raf.write(bytes);
                    System.out.println(Thread.currentThread() + " is done");
                    countDownLatch.countDown();
                    raf.close();
                    break;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            case 2:
                try {
                    buf.write(bytes);
                    System.out.println(Thread.currentThread() + " is done");
                    countDownLatch.countDown();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case 3:
                try {
                    dos.write(bytes);
                    System.out.println(Thread.currentThread() + " is done");
                    countDownLatch.countDown();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
        }
    }
}
