package main2;

import backtype.storm.Constants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.LOG;

public class KafkaCsvBoltv2 implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private HConnection hConnection;
    static final String TABLE_FRUIT = "fruit";
    static final byte[] COLUMN_FAMILY_FRUIT = "cf1".getBytes();
    static final byte[] COLUMN_COLOR = "COLOR".getBytes();
    static final byte[] COLUMN_COUNT = "COUNT".getBytes();
    private String metricsTableName;
    private String metricsTableCF;
    private HTableInterface metricsTable;
//    private static RestHighLevelClient restHighLevelClient;
//    private static final String HOST = "10.28.11.162";
//    private static final String HOST = "localhost";
//    private static final int PORT_ONE = 9200;
//    public static  final String SCHEME = "http";



    public KafkaCsvBoltv2(String metricsTableName, String metricsTableCF){
        this.metricsTableName = metricsTableName;
        this.metricsTableCF = metricsTableCF;
    }


    public static class ColumnName{
        private String retailer;
        private String PID;
        private String SKUV;

        public ColumnName(String retailer, String PID){
            this.retailer = retailer;
            this.PID = PID;
        }

        public ColumnName(String retailer, String PID, String SKUV){
            this.retailer = retailer;
            this.PID = PID;
            this.SKUV = SKUV;
        }

        public String retailer(){
            return retailer;
        }

        public String PID(){
            return PID;
        }

        public String SKUV(){
            return SKUV;
        }

        @Override
        public String toString(){
            return (retailer + " "+ PID + " "+ SKUV);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        Configuration config = HBaseConfiguration.create();
        try {
            hConnection = HConnectionManager.createConnection(config);
            this.metricsTableName = String.valueOf(hConnection.getTable(metricsTableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

//        makeConnection();
    }

//    private synchronized RestHighLevelClient makeConnection(){
//        if (restHighLevelClient == null) {
//            restHighLevelClient = new RestHighLevelClient(
//                    RestClient.builder(
//                            new HttpHost(HOST, PORT_ONE, SCHEME)
//                    ).setRequestConfigCallback(
//                            requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(0)
//                    ));
//        }
//        return restHighLevelClient;
//    }

    @Override
    public void execute(Tuple input) {
//Kafka tuples code
//        String [] arr = input.getValue(0).toString().replaceAll("\"","").split(",");
//        if(arr.length == 2){
//            ColumnName columnName = new ColumnName(arr[0], arr[1]);
//            getObjectData(columnName);
//
//        }else{
//            ColumnName columnName = new ColumnName(arr[0], arr[1], arr[2]);
//            getObjectData(columnName);
//
//        }
//        collector.ack(input);

        String key = "ORANGE";
        HTableInterface table = null;

        try {
            table = hConnection.getTable(TABLE_FRUIT);
            Get g = new Get(key.getBytes());
            Result result = table.get(g);
            String color = Bytes.toString(result.getValue(COLUMN_FAMILY_FRUIT,COLUMN_COLOR));

            System.out.println("\n\n\n\n\n");
            System.out.println(color+ "------------------------------------------------------");
            String count = Bytes.toString(  result.getValue(COLUMN_FAMILY_FRUIT, COLUMN_COUNT));

            System.out.println("\n\n\n\n\n");
            System.out.println(count+"--------------------------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
//                collector.ack(input);
            }
        }
    }

    public void getObjectData(ColumnName columnName){
        createProductMasterRowKey(columnName.retailer, columnName.PID, columnName.SKUV);
    }


    public void createProductMasterRowKey(String retailerName, String productId, String productSkuId){
        String ROWKEY_SEPARATOR = "^";
        StringBuilder rowKey = new StringBuilder();
        rowKey.append(DigestUtils.md5Hex(retailerName.toLowerCase()+ROWKEY_SEPARATOR+productId.toLowerCase())).append(ROWKEY_SEPARATOR);
        if(productSkuId != null && !productSkuId.isEmpty()){
            rowKey.append(DigestUtils.md5Hex(productSkuId.toLowerCase()));
        }
        System.out.println(rowKey);
    }

    @Override
    public void cleanup() {
        LOG.info("cleanup called");
        try {
            hConnection.close();
            LOG.info("hbase closed");
        } catch (Exception e) {
            LOG.error("cleanup error", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
