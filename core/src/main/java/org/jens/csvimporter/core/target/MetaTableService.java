package org.jens.csvimporter.core.target;

import org.apache.commons.lang3.StringUtils;
import org.jens.csvimporter.core.FileMeta;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.jdbc.ng.Table;
import org.jens.shorthand.stringutils.JavaTimeHelper;
import org.jens.shorthand.stringutils.MyTemplator;

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

    private static final String TABLE_META = "filemeta";
    private static final String TABLE_CONTENT = "content";

    MetaTableService(JdbcNG ng) {this.ng = ng;}

    private Table meta;
    private Table content;

    Table prepareMeta(Connection con) throws SQLException {
        Optional<Table> tableByName = findTableByName(con, TABLE_META);
        if (tableByName.isEmpty()) {
            try (Statement stm = con.createStatement()) {
                var sql = MyTemplator.template(
                    "create table ${TBL} (${ID} int primary key not null, ${PATH} varchar(500), ${FILENAME} varchar(500), ${FILEDATE} ${dt}, ${IMPORTED} ${dt})"
                );
                sql.param("TBL", ng.escapeTable(TABLE_META))
                    .param("ID", ng.escapeColumn("id"))
                    .param("PATH", ng.escapeColumn("path"))
                    .param("FILENAME", ng.escapeColumn("filename"))
                    .param("FILEDATE", ng.escapeColumn("filedate"))
                    .param("IMPORTED", ng.escapeColumn("imported"))
                    .param("dt", ng.getTimestampType());
                stm.execute(sql.get());
            }
            tableByName = findTableByName(con, TABLE_META);
        }
        var result = tableByName.orElseThrow(()->new IllegalStateException("unimplemented: konnte tabelle nicht erstellen ?"));
        this.meta = result;
        return result;
    }

    Table prepareContent(Connection con) throws SQLException {
        Optional<Table> tableByName = findTableByName(con, TABLE_CONTENT);
        if (tableByName.isEmpty()) {
            try (Statement stm = con.createStatement()) {
                MyTemplator sql = MyTemplator.template("create table ${TBL}( ${FILEID} int not null, ${LINENR} int not null)")
                    .param("TBL", ng.escapeTable(TABLE_CONTENT))
                    .param("FILEID", ng.escapeColumn("fileid"))
                    .param("LINENR", ng.escapeColumn("linenr"));

                stm.execute(sql.get());
            }
            tableByName = findTableByName(con, TABLE_CONTENT);
        }
        var result = tableByName.orElseThrow(()->new IllegalStateException("unimplemented: konnte tabelle nicht erstellen ?"));
        this.content = result;
        return result;
    }

    long addMeta(Connection con, FileMeta fileMeta) throws SQLException {
        long max = -1;
        MyTemplator queryMax = MyTemplator.template("select max(${ID}) from ${TBL}")
            .param("TBL", escapeTable(meta))
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
            .param("TBL", escapeTable(meta))
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

    String escapeTable(Table someTable) {
        return ng.escapeTable(someTable.schema()) + "." + ng.escapeTable(someTable.name());
    }

    Optional<Table> findTableByName(Connection con, String tableName) throws SQLException {
        var tmp = this.ng.getMeta().getAllTables(con, ng.getDb());
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

    Table getTableContent() {
        return this.content;
    }
}
