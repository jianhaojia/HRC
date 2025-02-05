package org.HFC.SQM.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author itcast
 * Date 2021/8/9 10:29
 * 读取 resource 资源目录下的配置文件中的 key / value
 * 开发步骤：
 * 1. 读取 resources / conf.properties
 * 2. 创建 Properties 用于接收 InputStream
 * 3. 创建获取字符串方法 根据输入key
 * 4. 创建获取数值方法，根据输入key
 */
public class ConfigLoader {
    //定义输出 logger 用于打印日志到控制台或者文件
    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    //1. 读取 resources / conf.properties
    private final static InputStream inputStream = ConfigLoader.class
            .getClassLoader()
            .getResourceAsStream("conf.properties");
    //2. 创建 Properties 用于接收 InputStream
    static Properties props = new Properties();

    // 通过静态代码块将当前的 inputStream 在类被初始化之前加载到 properties 中
    static {
        try {
            props.load(inputStream);
        } catch (IOException e) {
            logger.error("当前 ConfigLoad:static 出现异常: " + e.getMessage());
        }
    }
    //3. 创建获取字符串方法 根据输入key
    public static String getProperty(String key){
        String value = props.getProperty(key);
        return value;
    }
    //4. 创建获取数值方法，根据输入key
    public static int getInteger(String key){
        Object value = props.get(key);
        if(value != null || StringUtils.isNotEmpty(value.toString())) {
            return Integer.parseInt(value.toString());
        }else{
            return -999999;
        }
    }

    public static void main(String[] args) {
        int integer = getInteger("mongo.host");
        System.out.println(integer);
    }
}
