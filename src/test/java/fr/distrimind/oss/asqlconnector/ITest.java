package fr.distrimind.oss.asqlconnector;

import java.sql.Connection;
import java.sql.SQLException;

public interface ITest {
    void test(Connection conn) throws SQLException;
}
