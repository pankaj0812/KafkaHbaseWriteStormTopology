package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseConnection {
    private static volatile HConnection hConnection;
    private static final Logger logger = LoggerFactory.getLogger(HBaseConnection.class);

    private static HTableDescriptor table;

    private HBaseConnection(){
        logger.trace("HBaseConnection");
    }

    public static HConnection getConnection(){
        return hConnection;
    }

    public static HConnection createConnection(String hBaseZookeeperHostName) throws IOException{
        if(hConnection == null){
            synchronized (HBaseConnection.class){
                if(hConnection == null){
//                    logger.info("createConnection (hBase");
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", hBaseZookeeperHostName);
                    conf.set("hbase.zookeeper.property.clientPort", "2222");
                    hConnection = HConnectionManager.createConnection(conf);
                    logger.info("Returning Connection");

                }
            }
        }
        return hConnection;
    }

    public static void CloseConnection() throws IOException{
        logger.trace(">> closeConnection");
        if(hConnection != null){
            hConnection.close();
        }
    }
}
