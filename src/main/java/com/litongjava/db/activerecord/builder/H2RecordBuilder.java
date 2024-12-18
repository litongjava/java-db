package com.litongjava.db.activerecord.builder;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Config;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.activerecord.RecordBuilder;

/**
 * H2Database ResultRet to Record Builder.
 * <pre>
 * 使用示例：
 * H2Dialect dialect = new H2Dialect();
 * dialect.setRecordBuilder(H2RecordBuilder.me);
 * activeRecordPlugin.setDialect(dialect);
 * </pre>
 */
public class H2RecordBuilder extends RecordBuilder {

    public static final H2RecordBuilder me = new H2RecordBuilder();

    @Override
    public List<Row> build(Config config, ResultSet rs) throws SQLException {
        return build(config, rs, null);
    }

    /**
     * 处理h2database JDBC查询结果集到Record与oracle不同，h2database中 BLOB列数据直接getBytes()取数据不需要处理和转换
     *
     * @param config
     * @param rs
     * @param func
     * @return
     * @throws SQLException
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Row> build(Config config, ResultSet rs, Function<Row, Boolean> func) throws SQLException {
        List<Row> result = new ArrayList<Row>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        String[] labelNames = new String[columnCount + 1];
        int[] types = new int[columnCount + 1];
        buildLabelNamesAndTypes(rsmd, labelNames, types);
        while (rs.next()) {
            Row record = new Row();
            CPI.setColumnsMap(record, config.getContainerFactory().getColumnsMap());
            Map<String, Object> columns = record.getColumns();
            for (int i = 1; i <= columnCount; i++) {
                Object value;
                if (types[i] < Types.BLOB) {
                    value = rs.getObject(i);
                } else {
                    if (types[i] == Types.CLOB) {
                        value = rs.getString(i);
                    } else if (types[i] == Types.NCLOB) {
                        value = rs.getString(i);
                    } else if (types[i] == Types.BLOB) {
                        value = rs.getBytes(i);
                    } else {
                        value = rs.getObject(i);
                    }
                }
                columns.put(labelNames[i], value);
            }

            if (func == null) {
                result.add(record);
            } else {
                if (!func.apply(record)) {
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public void buildLabelNamesAndTypes(ResultSetMetaData rsmd, String[] labelNames, int[] types) throws SQLException {
        for (int i = 1; i < labelNames.length; i++) {
            // 备忘：getColumnLabel 获取 sql as 子句指定的名称而非字段真实名称
            labelNames[i] = rsmd.getColumnLabel(i);
            types[i] = rsmd.getColumnType(i);
        }
    }
}





