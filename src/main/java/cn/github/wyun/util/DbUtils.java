package cn.github.wyun.util;

import cn.hutool.core.text.CharSequenceUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DbUtils {

    private DbUtils() {
    }

    private static String url = "";
    private static String username = "";
    private static String password = "";

    public static void initDbUtil(String url1, String username1, String password1) {
        url = url1;
        username = username1;
        password = password1;
    }

    public static void initDbUtil(DbConfig dbConfig) {
        initDbUtil(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword());
    }

    public static void executeUpdateSQL(String sql) throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = DbUtils.getConnection();
            System.out.println(sql);
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeLargeUpdate();
        } finally {
            //关闭连接
            close(preparedStatement, connection);
        }
    }

    /**
     * 获取keyId
     *
     * @param sql sql
     * @return List<keyId>
     */
    public static List<Long> listKeyId(String sql) {
        return listColumnLong(sql, "key_id");
    }

    /**
     * 根据columnName获取某列
     *
     * @param sql        sql
     * @param columnName 列名
     * @return List<columnValue>
     */
    public static List<Long> listColumnLong(String sql, String columnName) {
        return listColumn(sql, columnName, Long.class);
    }

    /**
     * 根据columnName获取某列
     *
     * @param sql        sql
     * @param columnName 列明
     * @param columnType 列类型
     * @return list
     */
    public static <T> List<T> listColumn(String sql, String columnName, Class<T> columnType) {
        List<T> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet;
        try {
            connection = DbUtils.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                //获取keyID
                list.add(resultSet.getObject(columnName, columnType));
            }
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            //关闭连接
            close(preparedStatement, connection);
        }
        return list;
    }

    /**
     * 获取数据
     *
     * @param sql  sql
     * @param type 返回类
     * @return list
     */
    public static <T> List<T> list(String sql, Class<T> type) {
        List<T> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet;
        try {
            connection = DbUtils.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                //noinspection unchecked
                T newInstance = (T) Class.forName(type.getName()).newInstance();
                //获取定义了的字段
                Field[] fields = type.getDeclaredFields();
                Method[] declaredMethods = type.getDeclaredMethods();
                for (Field field : fields) {
                    String fieldName = field.getName();
                    Class<?> fieldType = field.getType();
                    // 字段名转下划线命名并获取值
                    Object objectValue = resultSet.getObject(CharSequenceUtil.toSymbolCase(fieldName, '_'), fieldType);
                    if (objectValue != null) {
                        // 找到对应的set方法
                        Method method = Arrays.stream(declaredMethods).
                                filter(it -> it.getName().equalsIgnoreCase("set" + fieldName)).findFirst().orElse(null);
                        if (method != null) {
                            method.invoke(newInstance, objectValue);
                        }
                    }
                }
                list.add(newInstance);
            }
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            //关闭连接
            close(preparedStatement, connection);
        }
        return list;
    }

    public static List<String> listAllDbName() {
        List<String> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet;
        try {
            connection = DbUtils.getConnection();
            preparedStatement = connection.prepareStatement("SHOW DATABASES");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                list.add(resultSet.getString("database"));
            }
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            //关闭连接
            close(preparedStatement, connection);
        }
        return list;
    }


    public static List<String> listAllTables(String clName) {
        List<String> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet;
        try {
            connection = DbUtils.getConnection();
            preparedStatement = connection.prepareStatement("show tables");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                list.add(resultSet.getString(clName));
            }
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            //关闭连接
            close(preparedStatement, connection);
        }
        return list;
    }

    public static String showCreateDdl(String sql) {
        List<String> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet;
        try {
            connection = DbUtils.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                list.add(resultSet.getString("Create Table"));
            }
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            //关闭连接
            close(preparedStatement, connection);
        }
        return list.isEmpty() ? "" : list.iterator().next();
    }

    /**
     * 获取数据库链接
     */
    private static Connection getConnection() {
        Connection connection;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    private static void close(PreparedStatement preparedStatement, Connection con) {
        if (null != preparedStatement) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException();
            } finally {
                if (null != con) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        System.err.println(e.getLocalizedMessage());
                    }
                }
            }
        }
    }
}
