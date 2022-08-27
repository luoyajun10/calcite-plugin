package org.apache.calcite.test;

import org.apache.calcite.ResultSetUtil;
import org.apache.calcite.csv.CsvSQLExecution;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CsvSQLSample {

    public static void main(String[] args) throws SQLException {
        String path = args[0];
        // initialize
        CsvSQLExecution sqlExecution = new CsvSQLExecution(path);
        // execute and process
        ResultSet resultSet = sqlExecution.doQuery(
                "select deptno,count(*) from csv.depts where empno = 110 group by deptno");
        System.out.println(ResultSetUtil.resultString(resultSet, true));
        // close
        sqlExecution.close();
    }
}
