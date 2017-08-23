package com.providertrust.im981;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static java.sql.ResultSet.*;

/**
 * Program to recreate audit log for addresslines
 * @author Russ Tennant (russ@proteus.co).
 */
public class Main
{
    static class Tuple
    {
        private final Integer _rev;
        private final long _addressLinesId;
        private final String _line;
        private Integer _revType;
        private Integer _orderId;
        private final int _rowNumber;

        Tuple(Integer rev, long addressLinesId, String line, Integer revType, Integer orderId, int rowNumber)
        {
            _rev = rev;
            _addressLinesId = addressLinesId;
            _line = line;
            _revType = revType;
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

        Integer getRevType()
        {
            return _revType;
        }

        Integer getRev()
        {
            return _rev;
        }

        long getAddressLinesId()
        {
            return _addressLinesId;
        }

        String getLine()
        {
            return _line;
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
                ", line='" + _line + '\'' + ", rev='" + _rev + '\'' +
                    ", revType='" + _revType + '\'' +
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
            PreparedStatement selectAudit = writeConn.prepareStatement(selectAuditSQL, TYPE_SCROLL_INSENSITIVE,
                    CONCUR_UPDATABLE);
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

    private static void reorderAuditTable(ArrayList<Tuple> tuples, Statement addressLineStmt,
                                          PreparedStatement selectAudit, PreparedStatement updateOrder)
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
                Tuple tuple = new Tuple(rev, addressLinesId, line, revType, orderId, rowNumber);
                tuples.add(tuple);
            }

            if (!processTuples(tuples, updateOrder))
            {
                log("Re-Order addresslines_aud ID = " + tuples.get(0).getAddressLinesId() + " and try again.");
                if (processTuples(reOrderTuples(tuples), updateOrder))
                    log("!!Success!!");
                else
                    log("!!Still failed!! Need manual fix for addresslines_aud ID = "
                            + tuples.get(0).getAddressLinesId());
            }

        }
    }

    private static boolean processTuples(ArrayList<Tuple> tupleList, PreparedStatement updateOrder)
    {
        final HashMap<Tuple, Integer> currentHistoryMap = new LinkedHashMap<>();
        final ArrayList<Tuple> currentTuples = new ArrayList<>();
        int order;

        for (Tuple tuple : tupleList) {
            int revType = tuple.getRevType();
            switch (revType) {
                case 0:
                    currentTuples.add(tuple);
                    order = currentTuples.size() - 1; // 0 based
                    // before we add a new line, we need to check if the new line order has already being taken
                    // by any previous existing line
                    if (order > 0 &&
                            currentTuples.stream().anyMatch(tuple1 -> (currentHistoryMap.get(tuple1) !=
                                    null && currentHistoryMap.get(tuple1) == currentTuples.size() - 1))) {
                        log("REV-DELETE: !!We guessed the wrong order!! " +
                                "We need to manually fix addresslines ID" + tuple);
                        return false;
                    }
                    currentHistoryMap.put(tuple, order);
                    if (tuple.getOrderId() == null || tuple.getOrderId() != order) {
                        log(String.format("REV-INSERT: Update order from %d to %d for %s",
                                (tuple.getOrderId() == null ? -1 : tuple.getOrderId()),
                                order,
                                tuple.toString()));
                        tuple.setOrderId(order);
                        executeUpdateOrderId(updateOrder, tuple, order);
                    }
                    break;
                case 1:
                    assert currentTuples.contains(tuple);
                    order = currentTuples.lastIndexOf(tuple) - 1; // 0 based
                    currentHistoryMap.put(tuple, order);
                    if (tuple.getOrderId() == null) {
                        log(String.format("REV-UPDATE: Update order from %d to %d for %s",
                                (tuple.getOrderId() == null ? -1 : tuple.getOrderId()),
                                order,
                                tuple.toString()));
                        executeUpdateOrderId(updateOrder, tuple, order);
                    }
                    break;
                case 2:
                    if (!currentTuples.remove(tuple)) {
                        log("REV-DELETE: !!DATA CORRUPTION!! Tuple doesn't exist -> " + tuple);
                        // Bailing - data needs to be fixed first
                        return true;
                    }

                    if (currentHistoryMap.get(tuple) != null
                            && !Objects.equals(tuple.getOrderId(), currentHistoryMap.get(tuple))) {
                        log(String.format("REV-DELETE: Re-order from %d to %d for %s",
                                (tuple.getOrderId() == null ? -1 : tuple.getOrderId()),
                                currentHistoryMap.get(tuple),
                                tuple.toString()));
                        executeUpdateOrderId(updateOrder, tuple, currentHistoryMap.get(tuple));
                    }
                    currentHistoryMap.remove(tuple);

                    break;
                default:
                    throw new AssertionError("What's a " + revType + " revType");
            }
        }
        return true;
    }

    private static void executeUpdateOrderId(PreparedStatement updateOrder, Tuple tuple, Integer orderId)
    {
        Savepoint savepoint = null;
        try
        {
            updateOrder.setInt(1, orderId);
            updateOrder.setInt(2, tuple.getRev());
            updateOrder.setLong(3, tuple.getAddressLinesId());
            updateOrder.setString(4, tuple.getLine());
            updateOrder.setInt(5, tuple.getRevType());
            savepoint = updateOrder.getConnection().setSavepoint();
            assert updateOrder.executeUpdate() == 1 : "Expected one row to be updated.";
            updateOrder.getConnection().releaseSavepoint(savepoint);
        }
        catch (SQLException e)
        {
            if (e.getSQLState().equals("23505"))
            {
                try {
                    if (savepoint != null)
                        updateOrder.getConnection().rollback(savepoint);
                } catch (SQLException e1) {
                    log("Can not rollback savepoint!");
                    e1.printStackTrace();
                }
            }

            log("!!SQLException, Can not update order_id!! May need manual fix: " + tuple);
        }
    }

    /**
     * Re-order the Tuples such that the first 2 consecutive revtype = 0 rows will switch their order
     * @param tuples the original tuples
     * @return the re-ordered tuples
     */
    private static ArrayList<Tuple> reOrderTuples(ArrayList<Tuple> tuples)
    {
        // first find the indexes of the first two consecutive revtype = 0
        int i, j;
        for (i = 0, j = 1; j < tuples.size(); i++, j++)
        {
            if (tuples.get(i).getRevType() == 0 && tuples.get(j).getRevType() == 0
                    && Objects.equals(tuples.get(i).getRev(), tuples.get(j).getRev()))
            {
                Collections.swap(tuples, i, j);
                break;
            }
        }

        return tuples;
    }

    static void log(String log)
    {
        System.out.println(log);
    }
}
