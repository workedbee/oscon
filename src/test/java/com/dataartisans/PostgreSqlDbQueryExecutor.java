package com.dataartisans;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.dataartisans.sinks.PostgreSqlDbConstants.*;

/**
 * PostgreSql database connection to execute queries
 */
public class PostgreSqlDbQueryExecutor {
    private Connection connection;

    public PostgreSqlDbQueryExecutor() {
        connect();
    }

    public void connect() {
        try {
            connection = DriverManager.getConnection(DEFAULT_DRIVER_URL, DEFAULT_LOGIN, DEFAULT_PASSWORD);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ResultSet query(String query) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            return preparedStatement.executeQuery();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
