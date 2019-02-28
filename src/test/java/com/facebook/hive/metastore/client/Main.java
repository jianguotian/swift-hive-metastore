package com.facebook.hive.metastore.client;

import com.facebook.swift.service.ThriftClientConfig;
import com.facebook.swift.service.ThriftClientManager;
import org.apache.hadoop.hive.metastore.api.Table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Main {
    public static void main(String args[]) throws Exception{
        String host = "localhost";
        int port = 9083;

        final HiveMetastoreClientConfig metastoreConfig = new HiveMetastoreClientConfig().setPort(port);
        try (final ThriftClientManager clientManager = new ThriftClientManager()) {
            final ThriftClientConfig clientConfig = new ThriftClientConfig();
            final HiveMetastoreFactory factory = new SimpleHiveMetastoreFactory(clientManager, clientConfig, metastoreConfig);

            try (final HiveMetastore metastore = factory.getDefaultClient()) {
                System.out.println(metastore.getAllDatabases());
                System.out.println(metastore.getAllTables("default"));
                Table table = metastore.getTable("default", "srcpart");
                System.out.println(table.getOwner());
            }
        }
    }
}
