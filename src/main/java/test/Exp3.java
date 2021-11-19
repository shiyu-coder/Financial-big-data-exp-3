package test;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class Exp3 {
    public static void main(String[] args){
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

        // 增加新的列族和新列Contact:Email，并添加数据
        HBaseOperator.addColumn("StuInfo", "Contact");
        HBaseOperator.addData("StuInfo", "2015001", "Contact", "Email", "lilie@qq.com");
        HBaseOperator.addData("StuInfo", "2015002", "Contact", "Email", "hmm@qq.com");
        HBaseOperator.addData("StuInfo", "2015003", "Contact", "Email", "zs@qq.com");

        // 删除学号为2015003的学生的选课记录
        HBaseOperator.delOneRecordFamily("StuInfo", "2015003", "Math");
        HBaseOperator.delOneRecordFamily("StuInfo", "2015003", "CS");
        HBaseOperator.delOneRecordFamily("StuInfo", "2015003", "English");

        // 删除所创建的表
        HBaseOperator.deleteTable("StuInfo");

    }
}
