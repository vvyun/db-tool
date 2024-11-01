package cn.github.wyun;

import cn.github.wyun.util.DbConfig;
import cn.github.wyun.util.DbUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 数据库数据对比工具
 */
public class DbDataCompare {
    // 忽略表后缀
    private static final String EXECUTIONS_END = "_history";
    // 同步脚本文件
    private static final File DDL_FILE = new File("./patch-data-ddl.sql");
    // 每次比较行数
    private static final int COMPARE_SIZE = 1000;
    // 主键名称
    private static final String PRIMARY_KEY = "key_id";
    // 配置名
    private static final String CONFIG_NAME = "compare.properties";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream input = DbDataCompare.class.getClassLoader().getResourceAsStream(CONFIG_NAME);
        properties.load(input);
        properties.list(System.out);
        // 数据库（原）
        final DbConfig dbSource = getDbConfig(properties, 1);
        // 数据库（待同步）
        final DbConfig dbTarget = getDbConfig(properties, 2);
        DbUtils.initDbUtil(dbSource);
        List<String> dbTablesSource = DbUtils.listAllTables("Tables_in_" + dbSource.getDbname());
        DbUtils.initDbUtil(dbTarget);
        List<String> dbTablesTarget = DbUtils.listAllTables("Tables_in_" + dbTarget.getDbname());

        // clear file
        Files.write(Paths.get(DDL_FILE.getPath()), "".getBytes(StandardCharsets.UTF_8));
        write2file("-- need modify table ---------------------------------------- ");

        List<String> dbTables = dbTablesSource.stream()
                .filter(dbTablesTarget::contains)
                .filter(it -> !it.endsWith(EXECUTIONS_END))
                .collect(Collectors.toList());
        for (String tableName : dbTables) {
            String countSql = "select count(1) from " + tableName;
            // use db source
            DbUtils.initDbUtil(dbSource);
            long size1 = DbUtils.countData(countSql);
            DbUtils.initDbUtil(dbTarget);
            if (size1 > COMPARE_SIZE * 100) {
                continue; // 暂时不支持超过10万的数据
            }
            // compare
            for (int i = 0; i < size1; i += COMPARE_SIZE) {
                DbUtils.initDbUtil(dbSource);
                String listSql = "select * from " + tableName + " limit " + i + "," + COMPARE_SIZE;
                System.out.println(listSql);
                List<Map<String, Object>> maps = DbUtils.listData(listSql);
                DbUtils.initDbUtil(dbTarget);
                for (Map<String, Object> map : maps) {
                    String keyId = String.valueOf(map.get(PRIMARY_KEY));
                    if (keyId != null) {
                        String listSqlById = "select * from " + tableName + " where " + PRIMARY_KEY + " = " + keyId;
                        List<Map<String, Object>> maps2sync = DbUtils.listData(listSqlById);
                        if (maps2sync.isEmpty()) {
                            // insert
                            String insert = "INSERT INTO " + tableName + " " + getInsertColumn(map) + ";";
                            write2file(insert);
                        } else {
                            // compare
                            Map<String, Object> map2syncData = maps2sync.get(0);
                            for (String string : map.keySet()) {
                                boolean b = compareNoPass(map.get(string), map2syncData.get(string));
                                if (b) {
                                    // update
                                    String update = "UPDATE " + tableName + " SET " + getUpdateColumn(map) + " WHERE " + PRIMARY_KEY + " = " + keyId;
                                    write2file(update);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    private static String getInsertColumn(Map<String, Object> map) {
        String sql = "(columns) VALUES (values);";
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (String column : map.keySet()) {
            if (map.get(column) != null && map.get(column).toString().length() < 100) { // 同步长度限制100
                columns.append(column).append(",");
                values.append("'").append(map.get(column)).append("',");
            }
        }
        columns.deleteCharAt(columns.length() - 1);
        values.deleteCharAt(values.length() - 1);
        return sql.replace("columns", columns.toString()).replace("values", values.toString());
    }

    private static String getUpdateColumn(Map<String, Object> map) {
        StringBuilder sql = new StringBuilder();
        for (String column : map.keySet()) {
            if (map.get(column) != null && map.get(column).toString().length() < 100) { // 同步长度限制100
                sql.append(column).append(" = '").append(map.get(column)).append("',");
            }
        }
        return sql.substring(0, sql.length() - 1);
    }

    private static DbConfig getDbConfig(Properties properties, int i) {
        // 数据库（原）
        return new DbConfig(
                properties.getProperty("db" + i + "userName"),
                properties.getProperty("db" + i + "password"),
                properties.getProperty("db" + i + "uri"),
                properties.getProperty("db" + i + "name")
        );
    }

    private static void write2file(String str) {
        try {
            System.out.println(str);
            str += "\n";
            Files.write(Paths.get(DDL_FILE.getPath()), str.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean compareNoPass(Object a, Object b) {
        if (a == null && b == null) {
            return false;
        }
        if (a == null || b == null) {
            return true;
        }
        return !a.equals(b);
    }

}
