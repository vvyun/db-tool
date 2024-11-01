package cn.github.wyun;

import cn.github.wyun.pojo.DBColumn;
import cn.github.wyun.util.DbConfig;
import cn.github.wyun.util.DbUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 数据库结构对比工具
 */
public class DbStructCompare {

    // 对比数据库名
    private static final String DB_NAME = "*";

    private static final String EXECUTIONS_END = "_history";

    private static final File file = new File("./patch-ddl.sql");

    public static void main(String[] args) throws IOException {

        Files.write(Paths.get(file.getPath()), "".getBytes(StandardCharsets.UTF_8));
        // 数据库（原）
        final DbConfig DB_SOURCE = new DbConfig(
                "*",
                "*",
                "*"
        );
        // 数据库（待同步）
        final DbConfig DB_TARGET = new DbConfig(
                "*",
                "*",
                "*"
        );

        Map<String, List<DBColumn>> sourceTableStruct = getTableStruct(
                DB_SOURCE
        );

        Map<String, List<DBColumn>> targetTableStruct = getTableStruct(
                DB_TARGET
        );
        // use DB_SOURCE
        DbUtils.initDbUtil(DB_SOURCE);
        // need remove table
        write2file("-- need remove table ---------------------------------------- ");
        targetTableStruct.keySet().forEach(tableName -> {
            if (!sourceTableStruct.containsKey(tableName)) {
                String str = "drop table " + tableName + ";";
                write2file(str);
            }
        });

        // need create table
        write2file("-- need create table ---------------------------------------- ");
        sourceTableStruct.keySet().forEach(tableName -> {
            if (!targetTableStruct.containsKey(tableName)) {
                String createDdlSql = "show create table `" + DB_NAME + "`." + tableName;
                String createTableDdl = DbUtils.showCreateDdl(createDdlSql);
                write2file(createTableDdl);
            }
        });
        // need modify table
        write2file("-- need modify table ---------------------------------------- ");
        sourceTableStruct.forEach((tableName, struct) -> {
            if (targetTableStruct.containsKey(tableName)) {

                String createDdlSql = "show create table `" + DB_NAME + "`." + tableName;
                String tableDdl = DbUtils.showCreateDdl(createDdlSql);
                String[] tableDdlLines = tableDdl.split("\n");

                List<DBColumn> targetStruct = targetTableStruct.get(tableName);
                // need remove columns
                for (DBColumn dbColumn : targetStruct) {
                    boolean b = struct.stream().anyMatch(it -> it.getCOLUMN_NAME().equals(dbColumn.getCOLUMN_NAME()));
                    if (!b) {
                        String str = "ALTER TABLE " + DB_NAME + "." + tableName + " DROP COLUMN " + dbColumn.getCOLUMN_NAME() + ";";
                        write2file(str);
                    }
                }

                // need add columns
                for (DBColumn dbColumn : struct) {
                    boolean b = targetStruct.stream().anyMatch(it -> it.getCOLUMN_NAME().equals(dbColumn.getCOLUMN_NAME()));
                    if (!b) {
                        genColumDdl(dbColumn, tableDdlLines, tableName, struct, "ADD");
                    }
                }
                // need update columns
                for (DBColumn dbColumn : struct) {
                    targetStruct.stream().filter(it -> it.getCOLUMN_NAME().equals(dbColumn.getCOLUMN_NAME()))
                            .findAny().ifPresent(targetColumn -> {
                                // compare status
                                if (compareNoPass(targetColumn.getCOLUMN_TYPE(), dbColumn.getCOLUMN_TYPE())
                                        || compareNoPass(targetColumn.getCOLUMN_DEFAULT(), dbColumn.getCOLUMN_DEFAULT())
                                        || compareNoPass(targetColumn.getCOLUMN_COMMENT(), dbColumn.getCOLUMN_COMMENT())
                                ) {
                                    genColumDdl(dbColumn, tableDdlLines, tableName, struct, "MODIFY");
                                }
                            });
                }
            }
        });
    }

    private static void write2file(String str) {
        try {
            System.out.println(str);
            str += "\n";
            Files.write(Paths.get(file.getPath()), str.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void genColumDdl(DBColumn dbColumn, String[] tableDdlLines, String table, List<DBColumn> struct, String opt) {
        String any = Arrays.stream(tableDdlLines)
                .filter(it -> it.contains("`" + dbColumn.getCOLUMN_NAME() + "`")).findAny().orElseThrow(null);
        any = any.trim().substring(0, any.trim().length() - 1);
        String addColumSql = "ALTER TABLE " + DbStructCompare.DB_NAME + "." + table + " " + opt + " " + any;
        int i = struct.indexOf(dbColumn);
        if (i > 1) {
            addColumSql += " AFTER " + struct.get(i - 1).getCOLUMN_NAME();
        }
        addColumSql += ";";
        write2file(addColumSql);
    }

    private static Map<String, List<DBColumn>> getTableStruct(DbConfig dbConfig) {
        DbUtils.initDbUtil(dbConfig);
        // 获取所有表
        List<String> dbTables = DbUtils.listAllTables("Tables_in_" + DbStructCompare.DB_NAME);
        dbTables = dbTables.stream()
                .filter(it -> !it.endsWith(EXECUTIONS_END))
                .collect(Collectors.toList());
        Map<String, List<DBColumn>> tableStruct = new HashMap<>();
        for (String tableName : dbTables) {
            List<DBColumn> dbColumns = listTableColum(tableName);
            tableStruct.put(tableName, dbColumns);
        }
        return tableStruct;
    }

    private static List<DBColumn> listTableColum(String tableName) {
        // 获取所有字段
        return DbUtils.list(
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + DbStructCompare.DB_NAME + "' AND TABLE_NAME = '" + tableName + "'",
                DBColumn.class
        );
    }


    private static boolean compareNoPass(String a, String b) {
        if (a == null && b == null) {
            return false;
        }
        if (a == null || b == null) {
            return true;
        }
        return !a.equals(b);
    }

}
