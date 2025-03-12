package org.HFC.SQM.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class MySQLDatabaseConnector {

    private Connection connection;

    public MySQLDatabaseConnector() {
        try {

            ConfigLoader configLoader = new ConfigLoader();
            String driver = configLoader.getProperty("jdbc.driver");
            String url = configLoader.getProperty("jdbc.url");
            String user = configLoader.getProperty("jdbc.user");
            String password = configLoader.getProperty("jdbc.password");

            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过 ID 查询厂区名称、车间名称和线名称
     *
     * @param id 要查询的记录的 ID
     * @return 包含厂区名称、车间名称和线名称的数组，如果查询失败返回 null
     */
    public String[] queryFactoryInfoById(int id) {
        String sql = "SELECT factory_name, workshop_name, line_name,notice_list,cc_list FROM machine WHERE machine_id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setInt(1, id);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    String factoryName = resultSet.getString("factory_name");
                    String workshopName = resultSet.getString("workshop_name");
                    String lineName = resultSet.getString("line_name");
                    String notice_list = resultSet.getString("notice_list");
                    String cc_list = resultSet.getString("cc_list");
                    return new String[]{factoryName, workshopName, lineName,notice_list,cc_list};
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MySQLDatabaseConnector connector = new MySQLDatabaseConnector();
        String[] info = connector.queryFactoryInfoById(1);
        if (info != null) {
            System.out.println("厂区名称: " + info[0]);
            System.out.println("车间名称: " + info[1]);
            System.out.println("线名称: " + info[2]);
        } else {
            System.out.println("未找到相关记录");
        }
        connector.closeConnection();
    }
}
