package test;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HBaseOperator {

    private static Configuration conf = null;
    private static Connection conn = null;
    private static Admin admin = null;
    public static AtomicInteger count = new AtomicInteger();

    static {
        conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "10.148.137.143");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    static {
        try {
            conn = ConnectionFactory.createConnection();
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tablename, String[] cfs){
        try{
            if(admin.tableExists(TableName.valueOf(tablename))){
                System.out.println("Table already exists!");
            }else{
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
                for(int i=0; i<cfs.length; i++){
                    HColumnDescriptor desc = new HColumnDescriptor(cfs[i]);
                    tableDesc.addFamily(desc);
                }
                admin.createTable(tableDesc);
                System.out.println("Create table: " + tablename + " ... Done.");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void deleteTable(String tablename){
        try{
            admin.disableTable(TableName.valueOf(tablename));
            admin.deleteTable(TableName.valueOf(tablename));
            System.out.println("Delete table:" + tablename + "... Done.");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void addData(String tableName, String rowKey, String family, String qualifier, String value){
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("Intert record ... Done.");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void getOneRecord(String tableName, String rowKey){
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            Result res = table.get(get);
            List<Cell> list = res.listCells();
            for(Cell cell:list){
                System.out.print(new String(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()) + " " );
                System.out.print(new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()) + ":" );
                System.out.print(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()) + " " );
                System.out.print(cell.getTimestamp() + " " );
                System.out.print(new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()) + " " );
                System.out.println("");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void delOneRecord(String tableName, String rowKey){
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            List<Delete> list = new ArrayList<Delete>();
            Delete del = new Delete(rowKey.getBytes());
            list.add(del);
            table.delete(list);
            System.out.println("Del record:" + rowKey + " ... Done.");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void delOneRecordFamily(String tableName, String rowKey, String family){
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            List<Delete> list = new ArrayList<Delete>();
            Delete del = new Delete(rowKey.getBytes());
            del.deleteFamily(Bytes.toBytes(family));
            list.add(del);
            table.delete(list);
            System.out.println("Del record:" + rowKey + "-" + family + " ... Done.");
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void addColumn(String tableName, String columnName){
        try{
            admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor cdesc = new HColumnDescriptor(columnName);
            desc.addFamily(cdesc);
            admin.addColumn(TableName.valueOf(tableName), cdesc);
            admin.enableTableAsync(TableName.valueOf(tableName));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void getByRawKeyColumn(String tableName,String rowKey, String column){
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            if(column.contains(":"))
            {
                //查询指定rowkey和列簇下的指定列名
                String[] split = column.split(":");
                get.addColumn(Bytes.toBytes(split[0]),Bytes.toBytes(split[1]));
                Result result = table.get(get);
                byte[] value = result.getValue(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
                if(Bytes.toString(value)!=null)
                    System.out.println(Bytes.toString(value));
                else
                    System.out.println("null");
            }
            else
            {
                //查询指定rowkey和列簇下的所有数据
                get.addFamily(column.getBytes());
                Result result = table.get(get);
                Cell[] cells = result.rawCells();
                for (Cell cell:cells)
                {
                    //获取列簇名称
                    String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    //获取列的名称
                    String colunmName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    //获取值
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    if(value!=null)
                        System.out.println(cf+":"+colunmName+"=>"+value);
                    else
                        System.out.println(cf+":"+colunmName+"=>"+"null");
                }
            }
            table.close();
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    public static List<KeyValue> getByFilter(String tableName, List<String> arr){
        List<KeyValue> res = new ArrayList<KeyValue>();
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            FilterList filterList = new FilterList();
            Scan s1 = new Scan();
            for(String v: arr){
                String[] s = v.split(",");
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]),
                        CompareFilter.CompareOp.EQUAL, Bytes.toBytes(s[2])));
//                s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
            }
            s1.setFilter(filterList);
            ResultScanner ResultScannerFilterList = table.getScanner(s1);
            for(Result rr = ResultScannerFilterList.next(); rr != null; rr = ResultScannerFilterList.next()){
                for(KeyValue kv: rr.list()){
                    res.add(kv);
//                    System.out.println("row-> " + new String(kv.getRow()));
//                    System.out.println("family:column-> " + new String(kv.getFamily()) + " : " + new String(kv.getQualifier()));
//                    System.out.println("value-> " + new String(kv.getValue()));
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return res;
    }

}
