//package util;
//
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.zookeeper.data.Stat;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class ConfigurationUtil {
//
//    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtil.class);
//
//    public static final HbaseInfo loadHbaseTables(CuratorFramework client, String basePath) throws Exception {
//        String applicationConfigPath = "/" + basePath + "/configurations/application/hbase";
//        try {
//            if (client.checkExists().forPath(applicationConfigPath) == null) {
//                LOG.error("Unable to load application configuration from zookeeper path: " + applicationConfigPath);
//                throw new RuntimeException("Unable to load application configuration from zookeeper path: " + applicationConfigPath);
//            }
//            HbaseInfo hbaseInfo = new HbaseInfo.HbaseInfoBuilder()
//                    .withRowKeyReferenceTable(new String(client.getData().forPath(applicationConfigPath + "/rowKeyReferenceTable")))
//                    .withHbaseZookeeperHost(new String(client.getData().forPath(applicationConfigPath + "/hbaseZookeeperHost")))
//                    .withValidationWarningTable(new String(client.getData().forPath(applicationConfigPath + "/validationWarningTable")))
//                    .withErrorTable(new String(client.getData().forPath(applicationConfigPath + "/errorTable")))
//                    .withPriceMasterTable(new String(client.getData().forPath(applicationConfigPath + "/priceMasterTable")))
//                    .withProductMasterTable(new String(client.getData().forPath(applicationConfigPath + "/productMasterTable")))
//                    .withProductInfoTable(new String(client.getData().forPath(applicationConfigPath + "/productInfo")))
//                    .withProductGraphTable(new String(client.getData().forPath(applicationConfigPath + "/productGraphTable")))
//                    .withProductGraphZookeeperHost(new String(client.getData().forPath(applicationConfigPath + "/productGraphZookeeperHost")))
//                    .withStatsTable(new String(client.getData().forPath(applicationConfigPath + "/statsTable")))
//                    .withDailyJobStatTable(new String(client.getData().forPath(applicationConfigPath + "/dailyStatsTable")))
//                    .withJobDetailsTable(new String(client.getData().forPath(applicationConfigPath + "/jobDetailsTable")))
//                    .withJobIdAttributesTable(new String(client.getData().forPath(applicationConfigPath + "/jobIdAttributeTable")))
//                    .withRetailerAttributeTable(new String(client.getData().forPath(applicationConfigPath + "/retailerAttributesTable")))
//                    .withRetailerRowkeyReferenceTable(new String(client.getData().forPath(applicationConfigPath + "/retailerRowKeyReferenceTable")))
//                    .withUrlReferenceTable(new String(client.getData().forPath(applicationConfigPath + "/urlReferenceTable")))
//                    .withSellerReferenceTable(new String(client.getData().forPath(applicationConfigPath + "/sellerReferenceTable")))
//                    .withSellerInfoTable(new String(client.getData().forPath(applicationConfigPath + "/sellerInfoTable")))
//                    .withSellerProductsTable(new String(client.getData().forPath(applicationConfigPath + "/sellerProductsTable")))
//                    .withProductSellersTable(new String(client.getData().forPath(applicationConfigPath + "/productSellersTable")))
//                    .withProductRelationTable(new String(client.getData().forPath(applicationConfigPath + "/productRelationTable")))
//                    .createHbaseInfo();
//            return hbaseInfo;
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println("Unable to load hbase tables ..........");
//            throw e;
//        }
//    }
//}
