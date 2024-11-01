package cn.github.wyun.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DbConfig {
    private String username;
    private String password;
    private String url;
    private String dbname;
}