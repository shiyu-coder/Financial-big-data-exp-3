# 金融大数据-实验3

施宇 191250119

## Windows下搭建伪分布式HBase环境

### 安装HBase

到[官网](http://archive.apache.org/dist/hbase/)下载HBase，这里选用的是1.2.0版本：

![image1](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/1.png)

下载完成后解压。打开hbase-1.2.0/conf文件夹，找到hbase-env.cmd文件，右键选择编辑，添加以下内容：

```shell
set HBASE_MANAGES_ZK=false
set JAVA_HOME=D:\Hadoop\Java7
set HBASE_CLASSPATH=D:\Hadoop\HBase\hbase-1.2.0\conf
```

其中`JAVA_HOME`和`HBASE_CLASSPATH`根据自己计算机上的Java和HBase路径进行配置。

找到hbase-site.xml文件，将内容替换为如下：

```xml
<configuration>
    <property>  
        <name>hbase.rootdir</name>  
        <value>file:///D:/Hadoop/HBase/hbase-1.2.0/root</value>  
    </property>  
    <property>  
        <name>hbase.tmp.dir</name>  
        <value>D:/Hadoop/HBase/hbase-1.2.0/tem</value>  
    </property>  
    <property>  
        <name>hbase.zookeeper.quorum</name>  
        <value>127.0.0.1</value>  
    </property>  
    <property>  
        <name>hbase.zookeeper.property.dataDir</name>  
        <value>D:/Hadoop/HBase/hbase-1.2.0/zoo</value>  
    </property>  
    <property>  
        <name>hbase.cluster.distributed</name>  
        <value>true</value>  
     </property>  
</configuration>

```

其中的路径也根据本机HBase路径进行配置。

### 启动HBase

启动Hadoop后，进入hbase-1.2.0/bin目录下，启动start-hbase.cmd。在cmd中到hbase-1.2.0/bin目录下，启动hbase shell：

```shell
hbase shell
```

出现

```shell
hbase(main):001.0>
```

说明搭建成功，Hadoop及HBase运行成功截图：

![image2](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/2.png)

### 配置java操作HBase项目依赖

在原来Hadoop的java项目的基础上，修改pom.xml文件，添加如下依赖：

```xml
<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-it</artifactId>
    <version>1.2.0</version>
</dependency>
<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-client</artifactId>
    <version>1.2.0</version>
</dependency>
```

## 编写Java程序完成下列任务

### 设计并创建合适的表

**原始数据如下：**

学生(student)

| 学号(S_No) | 姓名(S_Name) | 性别(S_Sex) | 年龄(S_Age) |
| :--------: | :----------: | :---------: | :---------: |
|  2015001   |    Li Lei    |    male     |     23      |
|  2015002   |  Han Meimei  |   female    |     22      |
|  2015003   |  Zhang San   |    male     |     24      |

课程(course)

| 课程号(C_No) |  课程名(C_Name)  | 学分(C_Credit) |
| :----------: | :--------------: | :------------: |
|    123001    |       Math       |      2.0       |
|    123002    | Computer Science |      5.0       |
|    123003    |     English      |      3.0       |

选课(sc)

| 学号(SC_Sno) | 课程号(SC_Cno) | 成绩(SC_Score) |
| :----------: | :------------: | :------------: |
|   2015001    |     123001     |       86       |
|   2015001    |     123003     |       69       |
|   2015002    |     123002     |       77       |
|   2015002    |     123003     |       99       |
|   2015003    |     123001     |       98       |
|   2015003    |     123002     |       95       |

**存储至HBase中：**原始数据的三张表可以转化为HBase中的一张表进行存储。

StuInfo表：

![image3](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/3.png)

其中学生信息、数学课信息、计算机课信息和英语课信息分别存储在Student、Math、CS和English三个列族中，使用学号作为rowKey。这样可以从一张表中查询到学生信息、课程信息和学生选课及成绩信息。

### 通过Java程序创建表

为了提高代码的复用性，创建HBaseOperator类，在该类中实现常用的HBase操作的函数，再通过调用这些函数完成后续任务。HBaseOperator类的所有方法都为静态方法，刚开始先静态初始化，完成HBase的连接操作。

```java
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

}
```

新建表格方法：

在确认要新建的表格不存在后，通过HTableDescriptor配置表格信息，增加列族，然后通过createTable新建表格：

```java
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
```

插入数据方法：

先通过getTable方法得到要插入的表格的Table对象，然后通过Put对象配置要插入的数据的rowKey，列族和列标签及值的信息，最后通过put方法插入数据。

```java
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
```

通过以上两个方法，可以完成表格的建立：

```java
// 创建表
HBaseOperator.createTable("StuInfo", new String[]{"Student", "Math", "CS", "English"});
// 添加学生信息
HBaseOperator.addData("StuInfo", "2015001", "Student", "S_No", "2015001");
HBaseOperator.addData("StuInfo", "2015001", "Student", "S_Name", "Li Lei");
HBaseOperator.addData("StuInfo", "2015001", "Student", "S_Sex", "male");
HBaseOperator.addData("StuInfo", "2015001", "Student", "S_Age", "23");

HBaseOperator.addData("StuInfo", "2015002", "Student", "S_No", "2015002");
HBaseOperator.addData("StuInfo", "2015002", "Student", "S_Name", "Han Meimei");
HBaseOperator.addData("StuInfo", "2015002", "Student", "S_Sex", "female");
HBaseOperator.addData("StuInfo", "2015002", "Student", "S_Age", "22");

HBaseOperator.addData("StuInfo", "2015003", "Student", "S_No", "2015003");
HBaseOperator.addData("StuInfo", "2015003", "Student", "S_Name", "Zhang San");
HBaseOperator.addData("StuInfo", "2015003", "Student", "S_Sex", "male");
HBaseOperator.addData("StuInfo", "2015003", "Student", "S_Age", "24");

// 添加课程信息
HBaseOperator.addData("StuInfo", "2015001", "Math", "C_No", "123001");
HBaseOperator.addData("StuInfo", "2015001", "Math", "C_Name", "Math");
HBaseOperator.addData("StuInfo", "2015001", "Math", "C_Credit", "2");
HBaseOperator.addData("StuInfo", "2015001", "English", "C_No", "123003");
HBaseOperator.addData("StuInfo", "2015001", "English", "C_Name", "English");
HBaseOperator.addData("StuInfo", "2015001", "English", "C_Credit", "3");

HBaseOperator.addData("StuInfo", "2015002", "CS", "C_No", "123002");
HBaseOperator.addData("StuInfo", "2015002", "CS", "C_Name", "Computer Science");
HBaseOperator.addData("StuInfo", "2015002", "CS", "C_Credit", "5");
HBaseOperator.addData("StuInfo", "2015002", "English", "C_No", "123003");
HBaseOperator.addData("StuInfo", "2015002", "English", "C_Name", "English");
HBaseOperator.addData("StuInfo", "2015002", "English", "C_Credit", "3");

HBaseOperator.addData("StuInfo", "2015003", "Math", "C_No", "123001");
HBaseOperator.addData("StuInfo", "2015003", "Math", "C_Name", "Math");
HBaseOperator.addData("StuInfo", "2015003", "Math", "C_Credit", "2");
HBaseOperator.addData("StuInfo", "2015003", "CS", "C_No", "123002");
HBaseOperator.addData("StuInfo", "2015003", "CS", "C_Name", "Computer Science");
HBaseOperator.addData("StuInfo", "2015003", "CS", "C_Credit", "5");

// 添加成绩信息
HBaseOperator.addData("StuInfo", "2015001", "Math", "SC_Score", "86");
HBaseOperator.addData("StuInfo", "2015001", "English", "SC_Score", "69");

HBaseOperator.addData("StuInfo", "2015002", "CS", "SC_Score", "77");
HBaseOperator.addData("StuInfo", "2015002", "English", "SC_Score", "99");

HBaseOperator.addData("StuInfo", "2015003", "Math", "SC_Score", "98");
HBaseOperator.addData("StuInfo", "2015003", "CS", "SC_Score", "95");
```

### 查询选修Computer Science的学生的成绩

查询某一列为某个值的数据可以通过Filter过滤器完成，因此实现根据Filter的表格查询方法：

通过SingleColumnValueFilter配置单列按值扫描，然后将配置好的Scan类传入Table中，扫描得到ResulterScanner，提取出其中的KeyValue作为函数的返回值返回即可。

```java
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
```

通过该函数得到所有CS:C_Name列的值为Computer Science的行以后，还需要通过训练提取出其中CS:SC_Score列的值，即考试成绩：

```java
// 查询选修Computer Science的学生的成绩
System.out.println("选修Computer Science的学生的成绩:");
List<String> arr = new ArrayList<String>();
arr.add("CS,C_Name,Computer Science");
List<KeyValue> res = HBaseOperator.getByFilter("StuInfo", arr);
for(KeyValue kv: res){
    if(new String(kv.getFamily()).equals("CS") && new String(kv.getQualifier()).equals("SC_Score")){
        System.out.print("S_No-> " + new String(kv.getRow()));
        System.out.println(" | SC_Score-> " + new String(kv.getValue()));
    }
}
```

查询结果为：

![image4](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/4.png)

### 增加新的列族和新列Contact:Email，并添加数据

增加新列可以直接通过addData，即插入数据的方法增加，而增加新的列族需要修改表的结构，这里实现增加新列族的方法：

通过HColumnDescriptor设定列族名称，然后通过addColumn方法添加列族。

```java
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
```

添加列族和新列，并加添数据的过程如下：

```java
// 增加新的列族和新列Contact:Email，并添加数据
HBaseOperator.addColumn("StuInfo", "Contact");
HBaseOperator.addData("StuInfo", "2015001", "Contact", "Email", "lilie@qq.com");
HBaseOperator.addData("StuInfo", "2015002", "Contact", "Email", "hmm@qq.com");
HBaseOperator.addData("StuInfo", "2015003", "Contact", "Email", "zs@qq.com");
```

### 删除学号为2015003的学生的选课记录

删除指定行的指定列族可以通过deleteFamily方法实现：

```java
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
```

删除学号为2015003的学生的选课记录即删除rowKey=2015003的行的Math、CS、English三个列族：

```java
// 删除学号为2015003的学生的选课记录
HBaseOperator.delOneRecordFamily("StuInfo", "2015003", "Math");
HBaseOperator.delOneRecordFamily("StuInfo", "2015003", "CS");
HBaseOperator.delOneRecordFamily("StuInfo", "2015003", "English");
```

### 删除所创建的表

删除表需要先通过disableTable方法将该表状态置为disable，然后通过deleteTable方法删除该表：

```java
public static void deleteTable(String tablename){
    try{
        admin.disableTable(TableName.valueOf(tablename));
        admin.deleteTable(TableName.valueOf(tablename));
        System.out.println("Delete table:" + tablename + "... Done.");
    }catch (IOException e){
        e.printStackTrace();
    }
}
```

删除创建的表StuInfo的过程：

```java
// 删除所创建的表
HBaseOperator.deleteTable("StuInfo");
```

除此之外，还有一些实现了但是本次实验没有用到的方法，包括获取指定行的数据(getOneRecord)，删除指定行(delOneRecord)，查询指定rowkey和列簇下的所有数据(getByRawKeyColumn)。

## 使用Shell完成上述Java程序的任务

### 创建表并插入数据

**创建表：**

```sql
create 'StuInfo', 'Student', 'Math', 'CS', 'English'
```

![image5](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/5.png)

**添加学生信息：**

```shell
put 'StuInfo', '2015001', 'Student:S_No', '2015001'
put 'StuInfo', '2015001', 'Student:S_Name', 'Li Lei'
put 'StuInfo', '2015001', 'Student:S_Sex', 'male'
put 'StuInfo', '2015001', 'Student:S_Age', '23'

put 'StuInfo', '2015002', 'Student:S_No', '2015002'
put 'StuInfo', '2015002', 'Student:S_Name', 'Han Meimei'
put 'StuInfo', '2015002', 'Student:S_Sex', 'female'
put 'StuInfo', '2015002', 'Student:S_Age', '22'

put 'StuInfo', '2015003', 'Student:S_No', '2015003'
put 'StuInfo', '2015003', 'Student:S_Name', 'Zhang San'
put 'StuInfo', '2015003', 'Student:S_Sex', 'male'
put 'StuInfo', '2015003', 'Student:S_Age', '24'
```

添加结果：

![image6](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/6.png)

**添加课程信息：**

```shell
put 'StuInfo', '2015001', 'Math:C_No', '123001'
put 'StuInfo', '2015001', 'Math:C_Name', 'Math'
put 'StuInfo', '2015001', 'Math:C_Credit', '2'
put 'StuInfo', '2015001', 'English:C_No', '123003'
put 'StuInfo', '2015001', 'English:C_Name', 'English'
put 'StuInfo', '2015001', 'English:C_Credit', '3'

put 'StuInfo', '2015002', 'CS:C_No', '123002'
put 'StuInfo', '2015002', 'CS:C_Name', 'Computer Science'
put 'StuInfo', '2015002', 'CS:C_Credit', '5'
put 'StuInfo', '2015002', 'English:C_No', '123003'
put 'StuInfo', '2015002', 'English:C_Name', 'English'
put 'StuInfo', '2015002', 'English:C_Credit', '3'

put 'StuInfo', '2015003', 'Math:C_No', '123001'
put 'StuInfo', '2015003', 'Math:C_Name', 'Math'
put 'StuInfo', '2015003', 'Math:C_Credit', '2'
put 'StuInfo', '2015003', 'CS:C_No', '123002'
put 'StuInfo', '2015003', 'CS:C_Name', 'Computer Science'
put 'StuInfo', '2015003', 'CS:C_Credit', '5'
```

添加结果：

![image7](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/7.png)

**添加成绩信息：**

```shell
put 'StuInfo', '2015001', 'Math:SC_Score', '86'
put 'StuInfo', '2015001', 'English:SC_Score', '69'

put 'StuInfo', '2015002', 'CS:SC_Score', '77'
put 'StuInfo', '2015002', 'English:SC_Score', '99'

put 'StuInfo', '2015003', 'Math:SC_Score', '98'
put 'StuInfo', '2015003', 'CS:SC_Score', '95'
```

添加结果：

![image8](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/8.png)

### 查询选修Computer Science的学生的成绩

```shell
scan 'StuInfo', {COLUMN=>'CS:SC_Score'}
```

查询结果：

![image9](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/9.png)

### 增加新的列族和新列Contact:Email，并添加数据

```shell
alter 'StuInfo', 'Contact'
put 'StuInfo', '2015001', 'Contact:Email', 'lilie@qq.com'
put 'StuInfo', '2015002', 'Contact:Email', 'hmm@qq.com'
put 'StuInfo', '2015003', 'Contact:Email', 'zs@qq.com'
```

结果：

![image10](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/10.png)

### 删除学号为2015003的学生的选课记录

```shell
delete 'StuInfo', '2015003', 'Math:C_No'
delete 'StuInfo', '2015003', 'Math:C_Name'
delete 'StuInfo', '2015003', 'Math:C_Credit'
delete 'StuInfo', '2015003', 'Math:SC_Score'
delete 'StuInfo', '2015003', 'CS:C_No'
delete 'StuInfo', '2015003', 'CS:C_Name'
delete 'StuInfo', '2015003', 'CS:C_Credit'
delete 'StuInfo', '2015003', 'CS:SC_Score'
delete 'StuInfo', '2015003', 'English:C_No'
delete 'StuInfo', '2015003', 'English:C_Name'
delete 'StuInfo', '2015003', 'English:C_Credit'
delete 'StuInfo', '2015003', 'English:SC_Score'
```

删除结果：

![image11](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/11.png)

### 删除所创建的表

```shell
disable 'StuInfo'
drop 'StuInfo'
```

删除结果：

![image12](https://github.com/shiyu-coder/Financial-big-data-exp-3/blob/master/image/12.png)
