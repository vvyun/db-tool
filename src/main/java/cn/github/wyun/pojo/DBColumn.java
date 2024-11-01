package cn.github.wyun.pojo;

import lombok.Data;

@Data
public class DBColumn {
    /**
     * 字段名
     */
    private String COLUMN_NAME;
    /**
     * 字段类型
     */
    private String COLUMN_TYPE;
    /**
     * 默认值
     */
    private String COLUMN_DEFAULT;
    /**
     * 字段类型
     */
    private String DATA_TYPE;
    /**
     * 是否为空
     */
    private String IS_NULLABLE;
    /**
     * 注释
     */
    private String COLUMN_COMMENT;
    /**
     * 字段长度
     */
    private String CHARACTER_MAXIMUM_LENGTH;
}
