package org.jens.csvimporter.core.target;

import org.jens.csvimporter.core.FileMeta;
import org.jens.csvimporter.core.MySpringRunner;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.jdbc.ng.Table;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Jens Ritter on 24.08.2024.
 */
class MetaTableServiceTest extends MySpringRunner {

    @Autowired
    private DataSource ds;
    private final Logger logger = LoggerFactory.getLogger(MetaTableServiceTest.class);


    @BeforeEach
    void setUp() throws SQLException {
        con = ds.getConnection();
        try (Statement stm = con.createStatement()) {
            var ng = JdbcNG.h2().build();
            for (Table aPublic : ng.getMeta().getAllTables(con, null, "PUBLIC")) {
                stm.execute("drop table " + MetaTableService.escapeTable(ng, aPublic));
            }
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        con.close();
    }

    private Connection con;

    abstract class MultiTests {
        private final String prefix;

        protected MultiTests(String prefix) {this.prefix = prefix;}


        @BeforeEach
        void setUpSub() {
            metaTableService = new MetaTableService(ng, prefix);
        }

        JdbcNG ng = JdbcNG.h2().build();
        MetaTableService metaTableService;

        @Test
        void prepareMeta() throws SQLException {
            Table table = metaTableService.prepareMeta(con);

            assertThat(table).isNotNull();
            assertThat(table.name()).isEqualTo(prefix != null ? prefix + MetaTableService.TABLE_META : MetaTableService.TABLE_META);
            assertThat(table.schema()).isEqualTo("PUBLIC"); // H2 schema

            Table table2 = metaTableService.prepareMeta(con);
            assertThat(table).isEqualTo(table2);
        }

        @Test
        void prepareContent() throws SQLException {
            Table table = metaTableService.prepareContent(con);

            assertThat(table).isNotNull();
            assertThat(table.name()).isEqualTo(prefix != null ? prefix + MetaTableService.TABLE_CONTENT : MetaTableService.TABLE_CONTENT);
            assertThat(table.schema()).isEqualTo("PUBLIC"); // H2 schema

            Table table2 = metaTableService.prepareContent(con);
            assertThat(table).isEqualTo(table2);


            Table table3 = metaTableService.getTableContent();
            assertThat(table).isEqualTo(table3);
        }

        @Test
        void addMeta() throws SQLException {
            metaTableService.prepareMeta(con);
            long id1 = metaTableService.addMeta(con, new FileMeta("path", "filename", LocalDateTime.now().withNano(0)));
            long id2 = metaTableService.addMeta(con, new FileMeta("path", "filename", LocalDateTime.now().withNano(0)));

            assertThat(id1).isLessThan(id2);
        }

        @Test
        void escapeTable() {
            var mock = mock(JdbcNG.class);
            doReturn("[schema]").when(mock).escapeTable("schema");
            doReturn("[table]").when(mock).escapeTable("table");

            var complete = new Table("db", "schema", "table", "tableType", "remakes");
            var h2 = new Table("db", null, "table", "tableType", "remakes");
            String s = MetaTableService.escapeTable(mock, complete);
            assertThat(s).isEqualTo("[schema].[table]");
        }

        @Test
        void findTableByName() throws SQLException {
            var uuid = UUID.randomUUID().toString();
            assertThat(metaTableService.findTableByName(con, uuid)).isEmpty();

            try (Statement stm = con.createStatement()) {
                stm.execute("create table " + JdbcNG.h2().build().escapeTable(uuid) + "(wert int)");
            }
            Optional<Table> tableByName = metaTableService.findTableByName(con, uuid);
            assertThat(tableByName).isPresent();
            Table table = tableByName.get();
            assertThat(table.name()).isEqualTo(uuid);

        }
    }

    @Nested
    @DisplayName("NoPrefix")
    public class NoPrefixTest extends MultiTests {

        NoPrefixTest() {
            super(null);
        }

    }

    @Nested
    @DisplayName("WithPrefix")
    public class WithPrefix extends MultiTests {

        WithPrefix() {
            super(UUID.randomUUID().toString());
        }
    }


}
