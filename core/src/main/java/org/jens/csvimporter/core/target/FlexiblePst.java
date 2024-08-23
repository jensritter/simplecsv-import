package org.jens.csvimporter.core.target;

import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.jdbc.ng.Table;
import org.jens.shorthand.stringutils.MyTemplator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Jens Ritter on 23.08.2024.
 */
public class FlexiblePst {

    private final PreparedStatement pst;
    private final int columns;

    private int batchCounter;

    FlexiblePst(JdbcNG ng, Connection con, long fileid, int columnCount, Table table) throws SQLException {
        this.columns = columnCount;
        this.pst = con.prepareStatement(insert(ng, table, fileid, columnCount));
    }

    private static String insert(JdbcNG ng, Table table, long fileid, int columnCount) {
        var sql = MyTemplator.template("insert into ${TBL} (${FIELDS}) values(${FILEID}, ${Q})");
        sql.param("TBL", MetaTableService.escapeTable(ng, table));
        sql.param("FILEID", fileid);

        Collection<String> insertFields = new ArrayList<>();
        insertFields.add("linenr");
        for (int i = 0; i < columnCount; i++) {
            insertFields.add(Integer.toString(i));
        }

        Collection<String> allFiels = new ArrayList<>();
        allFiels.add("fileid");
        allFiels.addAll(insertFields);

        sql.param("FIELDS", allFiels.stream()
            .map(ng::escapeColumn)
            .collect(Collectors.joining(","))
        );
        sql.param("Q", insertFields.stream().map(it->"?").collect(Collectors.joining(",")));

        return sql.toString();
    }

    public void addLine(int lineCount, Collection<String> line) throws SQLException {

        int counter = 1;
        pst.setInt(counter++, lineCount);
        for (String s : line) {
            pst.setString(counter++, s);
        }
        if (line.size() < columns) {
            for (int i = 0; i <= (columns - line.size()); i++) {
                pst.setString(counter++, null);
            }
        }
        pst.addBatch();
        if ((batchCounter++ % 1_000) == 0) {
            pst.executeBatch();
            pst.clearBatch();
        }
    }

    public void close() throws SQLException {
        pst.executeBatch();
        pst.close(); // pst muss immer gefüllt sein
    }
}
