package com.atguigu.hive.udtf;

import com.sun.org.apache.xpath.internal.Arg;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.http.util.Args;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:pengliangxing
 * @Date:2020/11/15
 */
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1.约束函数传入个数
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentLengthException("函数的参数个数只能为1！！！");
        }
            //2.约束函数传入参数类型
            String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
            if (!"string".equals(typeName)){
                throw new UDFArgumentTypeException(0,"函数第一个参数的类型只能为string！！！");
            }

        // 3. 约束函数返回值类型

        List<String> fieldName = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldName.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        //1.获取函数传入的JSONArray字符串
        String jsonArrayStr = objects[0].toString();
        //2.将jsonArray字符串转换成jsonString
        JSONArray jsonArray = new JSONArray(jsonArrayStr);
        //3.获取jsonArray里的一个个json，然后写出
        for (int i = 0; i < jsonArray.length(); i++) {
            String jsonStr = jsonArray.getString(i);
            //因为初始化方法里面限定了返回值类型是struct结构体
            //所以在这个地方不能直接输出action,需要用个字符串数组包装下
            String[] result = new String[1];
            result[0]=jsonStr;
            forward(result);


        }
    }

    @Override
    public void close() throws HiveException {

    }
}
