//package util;
//
//
//import org.apache.curator.RetryPolicy;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static util.ConfigurationUtil.loadHbaseTables;
//
//
//public class ApplicationConfiguration {
//    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfiguration.class);
////    private final HbaseInfo hbaseInfo;
//    private final String zkName;
//
//    public ApplicationConfiguration(String zkName, String basePath) {
//        this.zkName = zkName;
//        CuratorFramework client = null;
//        try {
//            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
//            client = CuratorFrameworkFactory.newClient(zkName, retryPolicy);
//            client.start();
//
////            hbaseInfo = loadHbaseTables(client, basePath);
//        } catch ( Exception e) {
//            LOG.error(" - Error " + e.getMessage());
//            throw new RuntimeException("Error parsing validation configuration", e);
//        } finally {
//            client.close();
//        }
//    }
//
//    public final HbaseInfo getHbaseInfo() {
//        return hbaseInfo;
//    }
//
//    @Override
//    public String toString() {
//        return "ApplicationConfiguration{" +
//                "hbaseInfo=" + hbaseInfo +
//                ", zkName='" + zkName + '\'' +
//                '}';
//    }
//}
