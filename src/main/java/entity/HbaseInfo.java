//package entity;
//
//public class HbaseInfo {
//    private String rowKeyReferenceTable;
//    private String productMasterTable;
//
//    public HbaseInfo(HbaseInfoBuilder hbaseInfoBuilder){
//        this.rowKeyReferenceTable = hbaseInfoBuilder.rowKeyReferenceTable.trim();
//        this.productMasterTable = hbaseInfoBuilder.productMasterTable.trim();
//    }
//
//    public String getRowKeyReferenceTable(){
//        return rowKeyReferenceTable;
//    }
//
//    public String getProductMasterTable(){
//        return productMasterTable;
//    }
//
//    public static class HbaseInfoBuilder{
//        private String rowKeyReferenceTable;
//        private String productMasterTable;
//
//        public HbaseInfoBuilder withRowKeyReferenceTable(String rowKeyReferenceTable){
//            this.rowKeyReferenceTable = rowKeyReferenceTable;
//            return this;
//        }
//
//        public HbaseInfoBuilder withProductMasterTable(String productMasterTable){
//            this.productMasterTable = productMasterTable;
//            return this;
//        }
//
//        public HbaseInfo createHbaseInfo(){
//            return new HbaseInfo(this);
//        }
//    }
//
//    @Override
//    public String toString(){
//        return "HbaseInfo{" +
//                "rowKeyReferenceTable='"+rowKeyReferenceTable
//    }
//}