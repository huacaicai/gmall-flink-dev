package com.zhang.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @title:
 * @author: zhang
 * @date: 2022/3/10 14:31
 */
public class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //定义返回json
        JSONObject result = new JSONObject();
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStruct = (Struct) valueStruct.get("source");
        Struct after = (Struct) valueStruct.get("after"); //影响的数据
        String database = sourceStruct.getString("db");
        String table = sourceStruct.getString("table");
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type = "insert";
        }
        JSONObject data = new JSONObject();
        if (after!=null){
            List<Field> fieldList = after.schema().fields();
            for (Field field : fieldList) {
                String key = field.name();
                Object value = after.get(field);
                data.put(key,value);
            }
        }
        result.put("database",database);
        result.put("table",table);
        result.put("type",type);
        result.put("data",data);



        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
