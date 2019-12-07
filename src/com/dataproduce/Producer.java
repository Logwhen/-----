package com.dataproduce;

import sun.security.util.BitArray;

import java.io.DataOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class Producer {
    public byte[]Data =new byte[2014*512*256*4];

public int produce()
{
    int curPosition=0;//记录当前位置的指针/
    byte[] bytes=new byte[4];

  for(int i=0;i<512*2014;i++)
  {
     for(int j=0;j<256;j++)
     {
       bytes=intToBytes(i+1);
       System.arraycopy(bytes,0, Data,curPosition,4);
       curPosition+=4;
     }
  }
return 1;
}
    public static byte[] intToBytes(int integer)
    {
        byte[] bytes=new byte[4];
        bytes[0]= (byte) (integer>>24);
        bytes[1]= (byte) (integer>>16);
        bytes[2]= (byte) (integer>>8);
        bytes[3]= (byte) integer;
        return bytes;
    }

}
