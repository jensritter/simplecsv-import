package org.jens.csvimporter.core.target;

import org.jens.csvimporter.core.FileMeta;
import org.jens.shorthand.jdbc.ng.Column;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.jdbc.ng.Table;
import org.jens.shorthand.stringutils.MyTemplator;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author Jens Ritter on 23.08.2024.
 */
public class JdbcImporter {
    private final JdbcNG ng;
    private final MetaTableService metaTableService;

    public JdbcImporter(JdbcNG ng, String prefix) {
        this.ng = ng;
        this.metaTableService = new MetaTableService(ng, prefix);
    }

    public Connection getCon() throws SQLException {
        var con = this.ng.getConnection();
        metaTableService.prepareTables(con);
        return con;
    }

    public FlexiblePst prepareInsert(@Nullable FlexiblePst pst, Connection con, long fileid, int size) throws SQLException {
        if (pst != null) {
            pst.close();
        }
        var contentTable = metaTableService.getTableContent();
        ensureColumns(con, contentTable, size);
        FlexiblePst flexiblePst = new FlexiblePst(ng, con, fileid, size, contentTable);
        return flexiblePst;
    }

    void ensureColumns(Connection con, Table tableContent, int csvColumns) throws SQLException {
        Map<String, Column> allColumns = ng.getMeta().getAllColumns(con, tableContent)
            .stream()
            .collect(Collectors.toMap(Column::name, it->it));

        Set<Integer> needToAppend = new TreeSet<>();
        for (int i = 0; i < csvColumns; i++) {
            if (!allColumns.containsKey(Integer.toString(i))) {
                needToAppend.add(i);
            }
        }
        if (needToAppend.isEmpty()) {
            return;
        }

        try (Statement stm = con.createStatement()) {
            for (Integer i : needToAppend) {
                String sql = appendColumn(tableContent, Integer.toString(i));
                stm.execute(sql);
            }
        }
    }

    String appendColumn(Table table, String columnName) {
        var sql = MyTemplator.template("alter table ${TBL} add ${COL} varchar(8000)");
        sql.param("TBL", MetaTableService.escapeTable(ng, table));
        sql.param("COL", ng.escapeColumn(columnName));
        return sql.toString();
    }


    public long addMeta(Connection con, FileMeta fileMeta) throws SQLException {
        long fileid = metaTableService.addMeta(con, fileMeta);
        return fileid;
    }


}
