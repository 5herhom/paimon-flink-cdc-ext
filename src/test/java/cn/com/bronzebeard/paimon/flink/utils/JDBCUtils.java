package cn.com.bronzebeard.paimon.flink.utils;

import cn.com.bronzebeard.paimon.flink.common.util.LogUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sherhomhuang
 * @date 2024/05/09 15:18
 * @description
 */
public class JDBCUtils {

    public static JSONArray executeQuery(Statement stmt, String sql){
        ResultSet resultSet = null;
        try {
            resultSet = stmt.executeQuery(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return resultSetToJSONArray(resultSet);
    }
    public static JSONArray resultSetToJSONArray(ResultSet rs) {
        try {
            JSONArray results = new JSONArray();
            List<String> colNames = new ArrayList<>();
            int columnCnt = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCnt; i++) {
                colNames.add(rs.getMetaData().getColumnName(i));
            }
            JSONObject row;
            while (rs.next()) {
                row = new JSONObject(true);
                results.add(row);
                for (int i = 1; i <= columnCnt; i++) {
                    row.put(colNames.get(i - 1), rs.getString(i));
                }
            }
            return results;
        } catch (Throwable e) {
            LogUtil.printStackTrace(e);
            throw new RuntimeException(e);
        }
    }
}
