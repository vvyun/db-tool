package cn.github.wyun.pojo;

import lombok.Data;

@Data
public class DBTable {
    /**
     * 数据库名
     */
    private String TABLE_SCHEMA;
    /**
     * 表名
     */
    private String TABLE_NAME;
    /**
     * 注释
     */
    private String TABLE_COMMENT;
}
