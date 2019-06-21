import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class kafkabolt extends BaseRichBolt{
    private OutputCollector collector;
    private String filePath = "/home/UGAM/pankaj.singh/IdeaProjects/Hbase_Storm_Kafka_ES/conf/sample.csv";
    int start = 0;
    LineIterator iterator;


    public static class ColumnName{
        private String retailer;
        private String PID;
        private String SKUV;

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

        public String SKUV() {
            return SKUV;
        }

        @Override
        public String toString(){
            return (retailer + " "+ PID + " "+SKUV);
        }

    }

    public static void main(String []args) throws IOException{
        parseCSV();
    }

    public static void parseCSV() throws IOException {
        CSVParser parser = new CSVParser(new FileReader("/home/UGAM/pankaj.singh/IdeaProjects/Hbase_Storm_Kafka_ES/conf/sample.csv"), CSVFormat.DEFAULT.withHeader());
        for(CSVRecord record:parser){
            createProductMasterRowkey(record.get("retailer"), record.get("PID"), record.get("SKUV"));
        }
        parser.close();
    }

    public static void createProductMasterRowkey(String retailerName, String productId, String productSkuId){
        String ROWKEY_SEPARATOR = "^";
        StringBuilder rowKey = new StringBuilder();
        rowKey.append(DigestUtils.md5Hex(retailerName.toLowerCase()+ROWKEY_SEPARATOR+productId.toLowerCase())).append(ROWKEY_SEPARATOR);
        if(productSkuId != null && !productSkuId.isEmpty()){
            rowKey.append(DigestUtils.md5Hex(productSkuId.toLowerCase()));
        }
    }



    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        Path path = Paths.get(filePath);
        try{
            iterator = FileUtils.lineIterator(path.toFile());
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        if(iterator.hasNext()){
            String message = iterator.next();
            start++;
            System.out.println("Emitted: " + start);
            collector.emit(new Values(message));
            start++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
    }
}
