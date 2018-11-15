package com.ulak;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient {

    private static Configuration CONFIG = null;

    public HbaseClient() throws IOException, ServiceException {
        {

            String filePath = "/Users/sercanyektas/hbasePerf/src/main/java/com/ulak/hbase-site.xml";

            Configuration conf = HBaseConfiguration.create();

            conf.addResource(new Path(filePath));
            try {
                HBaseAdmin.checkHBaseAvailable(conf);
                CONFIG = conf;

            } catch (MasterNotRunningException e) {
                System.out.println("HBase is not running." + e.getMessage());
            }
        }
    }

    public void createTable(String tableName) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        HTableDescriptor desc = new HTableDescriptor(tName);

        HColumnDescriptor family1 = new HColumnDescriptor(Bytes.toBytes("personal"));
        HColumnDescriptor family2 = new HColumnDescriptor(Bytes.toBytes("contactInfo"));
        desc.addFamily(family1);
        desc.addFamily(family2);
        HBaseAdmin admin = new HBaseAdmin(CONFIG);
        if(admin.tableExists(tableName)){
            System.out.println("Table already exists");
        } else {
            admin.createTable(desc);
            System.out.println("Table: " + tableName + " Successfully created");
        }
    }

    public void insertData(String tableName, String rowName) throws IOException {


        HTable hTable = new HTable(CONFIG, tableName);


        Put p = new Put(Bytes.toBytes(rowName));

        p.add(Bytes.toBytes("personal"),
                Bytes.toBytes("name"),Bytes.toBytes("Sercan"));
        p.add(Bytes.toBytes("personal"),
                Bytes.toBytes("age"),Bytes.toBytes("23"));
        p.add(Bytes.toBytes("contactInfo"),Bytes.toBytes("city"),
                Bytes.toBytes("İstanbul"));
        p.add(Bytes.toBytes("contactInfo"),Bytes.toBytes("country"),
                Bytes.toBytes("Turkey"));

        hTable.put(p);
    }

    public void readData(String tableName, String rowName) throws IOException {
        HTable hTable = new HTable(CONFIG, tableName);

        Get g = new Get(Bytes.toBytes(rowName));
        Result result = hTable.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));

        byte [] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("age"));

        byte [] value2 = result.getValue(Bytes.toBytes("contactInfo"),Bytes.toBytes("city"));

        byte [] value3 = result.getValue(Bytes.toBytes("contactInfo"),Bytes.toBytes("country"));

        // Printing the values
        String name = Bytes.toString(value);
        String age = Bytes.toString(value1);
        String city = Bytes.toString(value2);
        String country = Bytes.toString(value3);

        System.out.println("name: " + name + " age: " + age);
        System.out.println("city: " + city + " country: " + country );

    }
}
