package org.jens.csvimporter.core.target;

import org.jens.csvimporter.core.FileMeta;
import org.jens.csvimporter.core.MySpringRunner;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.jdbc.ng.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Jens Ritter on 24.08.2024.
 */
class JdbcImporterTest extends MySpringRunner {


    @BeforeEach
    void setUp() throws SQLException {
        jdbcImporter = new JdbcImporter(ng, null);
        this.con = jdbcImporter.getCon();
        tableContent = ng.getMeta().getAllTables(this.con, null, null, null)
            .stream().filter(it->it.name().equals(MetaTableService.TABLE_CONTENT))
            .findAny()
            .orElseThrow();
    }

    @AfterEach
    void tearDown() throws SQLException {
        this.con.close();
    }

    JdbcNG ng = JdbcNG.h2().build();
    JdbcImporter jdbcImporter;
    Connection con;
    Table tableContent = new Table("ignored", "PUBLIC", MetaTableService.TABLE_CONTENT, "type", "remarks");

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
        FlexiblePst sameSize = jdbcImporter.prepareInsert(bigger, con, 1, 2);

        var list = List.of("hi", "h2", "h3");
        assertThrows(SQLException.class, ()->{
            sameSize.addLine(2, list);
        });
        sameSize.close();
    }

    @Test
    void ensureColumns() throws SQLException {
        jdbcImporter.ensureColumns(con, tableContent, 1);
        jdbcImporter.ensureColumns(con, tableContent, 2);
        jdbcImporter.ensureColumns(con, tableContent, 3);
        jdbcImporter.ensureColumns(con, tableContent, 1);
        jdbcImporter.ensureColumns(con, tableContent, 2);
        assertThat(true).isTrue();
    }

    @Test
    void appendColumn() throws SQLException {
        String sql = jdbcImporter.appendColumn(tableContent, "JENS");
        try (Statement stm = con.createStatement()) {

            stm.execute(sql);
            try (ResultSet rs = stm.executeQuery("select count(JENS) from " + ng.escapeTable(MetaTableService.TABLE_CONTENT))) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(0);
            }
        }

    }

    @Test
    void addMeta() throws SQLException {
        MetaTableService mock = mock(MetaTableService.class);
        var jdbc = new JdbcImporter(mock);

        var con = mock(Connection.class);
        var filemeta = mock(FileMeta.class);
        long l = jdbc.addMeta(con, filemeta);
        verify(mock).addMeta(con, filemeta);

    }
}
