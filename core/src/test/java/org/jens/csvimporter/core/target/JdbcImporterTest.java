package org.jens.csvimporter.core.target;

import org.jens.csvimporter.core.MySpringRunner;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Jens Ritter on 24.08.2024.
 */
class JdbcImporterTest extends MySpringRunner {


    @BeforeEach
    void setUp() throws SQLException {
        jdbcImporter = new JdbcImporter(ng, null);
        this.con = jdbcImporter.getCon();
    }

    @AfterEach
    void tearDown() throws SQLException {
        this.con.close();
    }

    JdbcNG ng = JdbcNG.h2().build();
    JdbcImporter jdbcImporter;
    Connection con;


    @Test
    void getCon() {
        assertThat(con).isNotNull();
    }

    @Test
    void prepareInsert() throws SQLException {
        FlexiblePst flexiblePst = jdbcImporter.prepareInsert(null, con, 1, 1);
        flexiblePst.addLine(1, List.of("hi"));
        FlexiblePst bigger = jdbcImporter.prepareInsert(flexiblePst, con, 1, 2);
        bigger.addLine(2, List.of("hi"));
        bigger.addLine(2, List.of("hi", "h2"));

        var list = List.of("hi", "h2", "h3");
        assertThrows(SQLException.class, ()->{
            bigger.addLine(2, list);
        });
        bigger.close();
    }

    @Test
    void ensureColumns() {
    }

    @Test
    void appendColumn() {
    }

    @Test
    void addMeta() {
    }
}
