package org.jens.csvimporter.core.target;

import org.apache.commons.lang3.StringUtils;
import org.jens.csvimporter.core.FileMeta;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.jdbc.ng.Table;
import org.jens.shorthand.jdbc.ng.treiber.DbType;
import org.jens.shorthand.stringutils.JavaTimeHelper;
import org.jens.shorthand.stringutils.MyTemplator;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Erstellt, und verwaltet die SQL-Tabellen 'filemeta' und 'content'
 *
 * @author Jens Ritter on 23.08.2024.
 */
class MetaTableService {
    private final JdbcNG ng;

    static final String TABLE_META = "filemeta";
    static final String TABLE_CONTENT = "content";

    private final Optional<String> prefix;

    MetaTableService(JdbcNG ng, @Nullable String prefix) {
        this.ng = ng;
        this.prefix = Optional.ofNullable(prefix);
    }

    private Table meta;
    private Table tableContent;

    void prepareTables(Connection con) throws SQLException {
        this.prepareMeta(con);
        this.prepareContent(con);
    }

    Table prepareMeta(Connection con) throws SQLException {
        Optional<Table> tableByName = findTableByName(con, prefix.orElse("") + TABLE_META);
        if (tableByName.isEmpty()) {
            try (Statement stm = con.createStatement()) {
                var sql = MyTemplator.template(
                    "create table ${TBL} (${ID} int primary key not null, ${PATH} varchar(500), ${FILENAME} varchar(500), ${FILEDATE} ${dt}, ${IMPORTED} ${dt})"
                );
                sql.param("TBL", ng.escapeTable(prefix.orElse("") + TABLE_META))
                    .param("ID", ng.escapeColumn("id"))
                    .param("PATH", ng.escapeColumn("path"))
                    .param("FILENAME", ng.escapeColumn("filename"))
                    .param("FILEDATE", ng.escapeColumn("filedate"))
                    .param("IMPORTED", ng.escapeColumn("imported"))
                    .param("dt", ng.getTimestampType());
                stm.execute(sql.get());
            }
            tableByName = findTableByName(con, prefix.orElse("") + TABLE_META);
        }
        var result = tableByName.orElseThrow(()->new IllegalStateException("unimplemented: konnte tabelle nicht erstellen ?"));
        this.meta = result;
        return result;
    }

    Table prepareContent(Connection con) throws SQLException {
        Optional<Table> tableByName = findTableByName(con, prefix.orElse("") + TABLE_CONTENT);
        if (tableByName.isEmpty()) {
            try (Statement stm = con.createStatement()) {
                MyTemplator sql = MyTemplator.template("create table ${TBL}( ${FILEID} int not null, ${LINENR} int not null)")
                    .param("TBL", ng.escapeTable(prefix.orElse("") + TABLE_CONTENT))
                    .param("FILEID", ng.escapeColumn("fileid"))
                    .param("LINENR", ng.escapeColumn("linenr"));

                stm.execute(sql.get());
            }
            tableByName = findTableByName(con, prefix.orElse("") + TABLE_CONTENT);
        }
        var result = tableByName.orElseThrow(()->new IllegalStateException("unimplemented: konnte tabelle nicht erstellen ?"));
        this.tableContent = result;
        return result;
    }

    long addMeta(Connection con, FileMeta fileMeta) throws SQLException {
        long max;
        MyTemplator queryMax = MyTemplator.template("select max(${ID}) from ${TBL}")
            .param("TBL", escapeTable(ng, meta))
            .param("ID", ng.escapeColumn("id"));

        try (PreparedStatement pst = con.prepareStatement(queryMax.get())) {
            try (ResultSet resultSet = pst.executeQuery()) {
                if (!resultSet.next()) {throw new IllegalStateException("unimplemented: ");}
                max = resultSet.getLong(1) + 1;
            }
        }
        MyTemplator insert = MyTemplator.template(
                "insert into ${TBL} (${ID},${PATH},${FILENAME},${FILEDATE},${IMPORTED}) values(?,?,?,?,?)"
            )
            .param("TBL", escapeTable(ng, meta))
            .param("ID", ng.escapeColumn("id"))
            .param("PATH", ng.escapeColumn("path"))
            .param("FILENAME", ng.escapeColumn("filename"))
            .param("FILEDATE", ng.escapeColumn("filedate"))
            .param("IMPORTED", ng.escapeColumn("imported"));

        try (PreparedStatement pst = con.prepareStatement(insert.get())) {
            pst.setLong(1, max);
            pst.setString(2, fileMeta.path());
            pst.setString(3, fileMeta.filename());
            pst.setTimestamp(4, JavaTimeHelper.localDateTime2timestamp(fileMeta.modifed()));
            pst.setTimestamp(5, JavaTimeHelper.localDateTime2timestamp(LocalDateTime.now()));
            pst.executeUpdate();
            return max;
        }
    }

    static String escapeTable(JdbcNG jdbcNG, Table someTable) {
        if (someTable.schema() != null) {
            return jdbcNG.escapeTable(someTable.schema()) + "." + jdbcNG.escapeTable(someTable.name());
        } else {
            return jdbcNG.escapeTable(someTable.name());
        }
    }

    Optional<Table> findTableByName(Connection con, String tableName) throws SQLException {
        String dbName = ng.getDb();
        if (ng.getType() == DbType.H2FILE) {
            String filenameOnly = new File(dbName).getName();
            dbName = filenameOnly.toUpperCase(Locale.ROOT);
        }
        var tmp = this.ng.getMeta().getAllTables(con, dbName);
        List<Table> list = tmp
            .stream()
            .filter(it->it.name().toLowerCase(Locale.ROOT).equals(tableName.toLowerCase(Locale.ROOT)))
            .toList();
        if (list.isEmpty()) {return Optional.empty();}
        if (list.size() == 1) {
            return Optional.of(list.get(0));
        }

        String schema;
        if (StringUtils.isBlank(this.ng.getSchema())) {
            schema = ng.getDefaultSchema();
        } else {
            schema = ng.getSchema();
        }
        return list.stream().filter(it->it.schema().toLowerCase(Locale.ROOT).equals(schema)).findAny();
    }

    Table getTableContent() {return this.tableContent;}
}
