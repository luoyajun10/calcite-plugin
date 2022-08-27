package org.apache.calcite;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CalciteUtil {

    public static CalciteConnection getConnection(Properties config) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
        return connection.unwrap(CalciteConnection.class);
    }
}
