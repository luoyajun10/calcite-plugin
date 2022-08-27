package org.apache.calcite.csv;

import org.apache.calcite.CalciteUtil;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.sql.*;
import java.util.Objects;
import java.util.Properties;

public class CsvSQLExecution {

    private Boolean caseSensitive = false;
    private CalciteConnection calciteConnection;

    public CsvSQLExecution(String fileDirectory) {
        try {
            Properties config = new Properties();
            config.setProperty("caseSensitive", caseSensitive.toString());
            this.calciteConnection = CalciteUtil.getConnection(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Calcite Connection! Message: " + e.getMessage());
        }
        buildSchema(new CsvSchema(new File(fileDirectory), CsvTable.Flavor.SCANNABLE));
    }

    private void buildSchema(Schema schema) {
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("csv", schema);
    }

    public ResultSet doQuery(String sqlText) {
        try {
            Statement statement = calciteConnection.createStatement();
            return statement.executeQuery(sqlText);
        } catch (SQLException e) {
            throw new RuntimeException("Execute SQL exception! Message: " + e.getMessage());
        }
    }

    public void close() {
        try {
            if (Objects.nonNull(calciteConnection)) {
                calciteConnection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Close connection error! Message: " + e.getMessage());
        }
    }
}
