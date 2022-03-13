package com.zhang.gmall.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhang.gmall.beans.TableProcess;
import com.zhang.gmall.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @title:
 * @author: zhang
 * @date: 2022/3/10 15:29
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> dimTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // {"database":"gmall_realtime_2022","data":{" "},"type":"insert","table":"table_process"}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1.解析参数，封装key，v 写入状态
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject data = jsonObject.getJSONObject("data");
        TableProcess tableProcess = JSON.toJavaObject(data, TableProcess.class);
        String sourceTable = tableProcess.getSourceTable();
        String operateType = tableProcess.getOperateType();
        String key = sourceTable + "-" + operateType;
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key, tableProcess);
        //TODO 3.如果是维度数据则提前创建Phoenix表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //创建表
            generateTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

    }


    //{"database":"gmall_flink_2022","table":"user_info","type":"update","ts":1646138535,"xid":36020,"xoffset":30,"data":{},"old":{}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //TODO 2.封装key获取广播状态信息
        String table = value.getString("table");
        String type = value.getString("type");
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
        }
        String key = table + "-" + type;
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);

        //TODO 4.根据配置表对象，对主流数据进行分流
        //维度数据--hbase、实时数据--kafka对应主题
        if (tableProcess != null) {
            String sinkType = tableProcess.getSinkType();
            //为所有数据添加目的地信息
            String sinkTable = tableProcess.getSinkTable();
            value.put("sinkTable",sinkTable);

            //根据配置表过滤业务数据字段
            JSONObject data = value.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumns(data,sinkColumns);

            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //维度数据发送到hbase,侧输出流
                ctx.output(dimTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //事实数据发送到kafka，主流数据
                out.collect(value);
            }
        }else{
            //说明配置表没有要处理的表信息
            System.out.println("该组合key：" + key + "不存在！");
        }


    }

    /**
     * 过滤字段
     * @param data 业务数据字段
     * @param sinkColumns 配置表字段
     */
    private void filterColumns(JSONObject data, String sinkColumns) {
        String[] arr = sinkColumns.split(",");
        List<String> fields = Arrays.asList(arr);
        Set<String> keySet = data.keySet();
        keySet.removeIf(field->!fields.contains(field));
    }


    /**
     * 创建Phoenix表
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     * @throws SQLException
     */
    private void generateTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        PreparedStatement statement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            //创建最终sql
            StringBuilder createSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("( ");

            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                createSQL.append(field);
                if (field.equals(sinkPk)) {
                    createSQL.append(" VARCHAR primary key ");
                } else {
                    createSQL.append(" VARCHAR ");
                }
                if (i < fields.length - 1) {
                    createSQL.append(",");
                }
            }
            createSQL.append(") ").append(sinkExtend);
            System.out.println("Phoenix建表语句：" + createSQL);
            //获取数据库操作对象
            statement = connection.prepareStatement(createSQL.toString());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix建表失败！");
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

}
