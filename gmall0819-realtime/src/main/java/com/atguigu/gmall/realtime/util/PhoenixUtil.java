package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-23 18:21
 * 查询Phoenix的工具类
 */
public class PhoenixUtil {
    private static Connection conn;

    public static void initConnection() throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //设置操作的schema
        conn.setSchema(GmallConfig.HBASE_SCHEMA);
    }

    /**
     * 从Phoenix表中查询数据
     *
     * @param sql 要执行的查询语句
     * @param clz 将查询的结果封装为哪种类型
     * @return 查询结果集合
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> resList = new ArrayList<>();
        try {
            if (conn == null) {
                initConnection();
            }
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
           /*
           //处理结果集
            +-----+-----------+
            | ID  |  TM_NAME  |
            +-----+-----------+
            | 12  | chails    |
            | 16  | bbb       |
            */
            //获取结果对应的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();

            //对查询结果进行遍历
            while (rs.next()) {
                //创建要封装的对象
                T obj = clz.newInstance();
                //对查询结果集的每一列进行遍历
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) {
        List<JSONObject> jsonObjList = queryList("select * from dim_base_trademark", JSONObject.class);
        System.out.println(jsonObjList);
    }
}
