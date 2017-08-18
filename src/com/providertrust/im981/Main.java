package com.providertrust.im981;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import static java.sql.ResultSet.*;

/**
 * Program to recreate audit log for addresslines
 * @author Russ Tennant (russ@proteus.co).
 */
public class Main
{
    static class Tuple
    {
        private final long _addressLinesId;
        private final String _line;
        private Integer _orderId;
        private final int _rowNumber;

        Tuple(long addressLinesId, String line, Integer orderId, int rowNumber)
        {
            _addressLinesId = addressLinesId;
            _line = line;
            _orderId = orderId;
            _rowNumber = rowNumber;
        }

        Integer getOrderId()
        {
            return _orderId;
        }

        void setOrderId(Integer orderId)
        {
            _orderId = orderId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple tuple = (Tuple) o;
            return _addressLinesId == tuple._addressLinesId &&
                Objects.equals(_line, tuple._line);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(_addressLinesId, _line);
        }

        @Override
        public String toString()
        {
            return "Tuple{" + "addressLinesId=" + _addressLinesId +
                ", line='" + _line + '\'' +
                ", rowNumber=" + _rowNumber +
                '}';
        }
    }

    private final DataSource _dataSource;

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception on error.
     */
    public static void main(String[] args) throws Exception
    {
        Map<String, String> env = System.getenv();
        String pguser = env.get("PGUSER");
        String pgpassword = env.get("PGPASSWORD");
        String pgdatabase = env.get("PGDATABASE");
        String pghost = env.get("PGHOST");

        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUser(pguser);
        dataSource.setPassword(pgpassword);
        dataSource.setDatabaseName(pgdatabase);
        dataSource.setServerName(pghost);
        try(Connection connection = dataSource.getConnection();
            Statement stmt = connection.createStatement())
        {
            stmt.execute("SELECT 1");
        }

        Main main = new Main(dataSource);
        main.run();
    }

    Main(DataSource dataSource)
    {
        _dataSource = dataSource;
    }

    private void run() throws Exception
    {
        ArrayList<Tuple> tuples = new ArrayList<>();
        String selectAuditSQL =
            "select rev, addresslines, revtype, order_id, "
            + "row_number() OVER (ORDER BY rev, revtype desc, order_id), addresslines_id "
            + "FROM ptgrid.addresslines_aud WHERE "
            + "addresslines_id = ? ORDER BY rev, revtype desc, order_id";

        String updateOrderSQL = "UPDATE ptgrid.addresslines_aud SET order_id = ? \n"
            + "WHERE rev = ? AND addresslines_id = ? AND addresslines = ? AND revtype = ?";


        try(Connection readOnlyConn = _dataSource.getConnection();
            Statement addressLineStmt = readOnlyConn.createStatement();
            Connection writeConn = _dataSource.getConnection();
            PreparedStatement selectAudit = writeConn.prepareStatement(selectAuditSQL, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE);
            PreparedStatement updateOrder = writeConn.prepareStatement(updateOrderSQL))
        {
            readOnlyConn.setReadOnly(true);
            readOnlyConn.setAutoCommit(false);
            writeConn.setAutoCommit(false);
            addressLineStmt.setFetchSize(50);
            addressLineStmt.setFetchDirection(FETCH_FORWARD);

            reorderAuditTable(tuples, addressLineStmt, selectAudit, updateOrder);
            writeConn.commit();
        }
    }

    private static void reorderAuditTable(ArrayList<Tuple> tuples, Statement addressLineStmt, PreparedStatement selectAudit,
        PreparedStatement updateOrder)
        throws SQLException
    {
        try (ResultSet gridAddressIdResult = addressLineStmt.executeQuery(//WHERE addresslines_id = 429854
            "select distinct addresslines_id from ptgrid.addresslines_aud  "
                + "GROUP BY addresslines_id, rev having count(rev) > 1 ORDER BY addresslines_id"))
        {
            while (gridAddressIdResult.next())
            {
                tuples.clear();
                long addressLinesId = gridAddressIdResult.getLong(1);
                selectAudit.setLong(1, addressLinesId);
                reorderRecord(tuples, selectAudit, updateOrder, addressLinesId);
            }
        }
    }

    private static void reorderRecord(ArrayList<Tuple> tuples, PreparedStatement selectAudit,
        PreparedStatement updateOrder, long addressLinesId)
        throws SQLException
    {
        try (ResultSet auditResult = selectAudit.executeQuery())
        {
            final int ORDER_COLUMN = 4;
            while (auditResult.next())
            {
                int rev = auditResult.getInt(1);
                String line = auditResult.getString(2);
                int revType = auditResult.getInt(3);
                Integer orderId = auditResult.getInt(ORDER_COLUMN);
                if (auditResult.wasNull())
                    orderId = null;
                int rowNumber = auditResult.getInt(5);
                Tuple tuple = new Tuple(addressLinesId, line, orderId, rowNumber);
                switch (revType)
                {
                    case 0:
                        tuples.add(tuple);
                        int order = tuples.size() - 1; // 0 based
                        if (orderId == null || orderId != order)
                        {
                            tuple.setOrderId(order);
                            auditResult.updateInt(ORDER_COLUMN, order);
                            auditResult.updateRow();
                            log(String.format("REV-INSERT: Update order from %d to %d for %s",
                                (orderId == null ? -1 : orderId),
                                order,
                                tuple.toString()));
                        }
                        break;
                    case 1:
                        assert tuples.contains(tuple);
                        order = tuples.lastIndexOf(tuple) - 1; // 0 based
                        if (orderId == null)
                        {
                            auditResult.updateInt(ORDER_COLUMN, order);
                            auditResult.updateRow();
                        }
                        break;
                    case 2:
                        if(!tuples.remove(tuple))
                        {
                            log("REV-DELETE: !!DATA CORRUPTION!! Tuple doesn't exist -> " + tuple);
                            // Bailing - data needs to be fixed first
                            return;
                        }

                        if (!tuples.isEmpty())
                        {
                            for (int i = 0; i < tuples.size(); i++)
                            {
                                Tuple toCheck = tuples.get(i);
                                //noinspection UnnecessaryUnboxing
                                if (toCheck.getOrderId() == null || toCheck.getOrderId().intValue() != i)
                                {
                                    log(String.format("REV-DELETE: Re-order from %d to %d for %s",
                                        (toCheck.getOrderId() == null ? -1 : toCheck.getOrderId()),
                                        i,
                                        toCheck.toString()));
                                    toCheck.setOrderId(i);
                                    updateOrder.setInt(1, i);
                                    updateOrder.setInt(2, rev);
                                    updateOrder.setLong(3, addressLinesId);
                                    updateOrder.setString(4, line);
                                    updateOrder.setInt(5, revType);
                                    assert updateOrder.executeUpdate() == 1 : "Expected one row to be updated.";
                                }
                            }
                        }

                        break;
                    default:
                        throw new AssertionError("What's a " + revType + " revType");
                }
            }
        }
    }

    static void log(String log)
    {
        System.out.println(log);
    }
}
