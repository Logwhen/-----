package com.dataproduce;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CAWriter {
    public static Cluster cluster;
    public static Session session;
    public static void main(String args[])
    {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder().addContactPoint("49.234.62.35").withPort(9042).build();
            Session session = cluster.connect();                                          // (2)
            ResultSet rs = session.execute("select release_version from system.local");    // (3)
            Row row = rs.one();
            System.out.println(row.getString("release_version"));                          // (4)
        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }

}


}
