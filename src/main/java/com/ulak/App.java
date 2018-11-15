package com.ulak;


import com.google.protobuf.ServiceException;

import java.io.IOException;


public class App 
{
    public static void main( String[] args ) throws IOException, ServiceException {
        HbaseClient client = new HbaseClient();

        String tableName = "Employee";
        String rowName = "row1";

        client.createTable(tableName);
        client.insertData(tableName, rowName);
        client.readData(tableName, rowName);


    }

}
