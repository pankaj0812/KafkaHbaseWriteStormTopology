package main2;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.commons.codec.digest.DigestUtils;
import java.util.Map;


public class KafkaCsvBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;



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
    }

    @Override
    public void execute(Tuple input) {

        String [] arr = input.getValue(0).toString().replaceAll("\"","").split(",");
        if(arr.length == 2){
            ColumnName columnName = new ColumnName(arr[0], arr[1]);
            getObjectData(columnName);

        }else{
            ColumnName columnName = new ColumnName(arr[0], arr[1], arr[2]);
            getObjectData(columnName);

        }
        collector.ack(input);
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

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
