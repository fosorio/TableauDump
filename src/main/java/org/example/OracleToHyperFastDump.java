package org.example;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.io.File;
import com.tableau.hyperapi.*;

import static java.sql.JDBCType.BINARY;

public class OracleToHyperFastDump {

    // Special object to signal completion
    private static final List<Object[]> POISON_PILL = Collections.emptyList();

    public static void main(String[] args) {
        // Oracle database connection parameters
        String jdbcUrl = "jdbc:oracle:thin:@//localhost:1521/FREE";
        String username = "C##FOSOR";
        String password = "qwe";
        String tableName = "TABLEAUT"; // Specify your table name here

        // Number of reader threads (adjust based on your system)
        int numThreads = Runtime.getRuntime().availableProcessors();

        // Hyper file path
        String hyperFilePath = "output.hyper";

        // BlockingQueue to hold data batches between readers and writer
        BlockingQueue<List<Object[]>> queue = new ArrayBlockingQueue<>(100);

        // Variables to hold table metadata
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        String idColumnName = null;

        // Fetch table metadata and min/max ID values
        long minId = 0;
        long maxId = 0;
        try (java.sql.Connection oracleConnection = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData dbMetaData = oracleConnection.getMetaData();

            // Get column metadata
            ResultSet columns = dbMetaData.getColumns(null, username.toUpperCase(), tableName.toUpperCase(), null);
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int dataType = columns.getInt("DATA_TYPE");
                String typeName = columns.getString("TYPE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                boolean isNullable = "YES".equals(columns.getString("IS_NULLABLE"));

                ColumnMetadata columnMeta = new ColumnMetadata(columnName, dataType, typeName, columnSize, decimalDigits, isNullable);
                columnMetadataList.add(columnMeta);
            }

            if (columnMetadataList.isEmpty()) {
                System.err.println("Failed to retrieve column metadata for table " + tableName);
                return;
            }

            // Identify a suitable ID column for partitioning
            // Here, we look for a numeric column (preferably primary key)
            ResultSet primaryKeys = dbMetaData.getPrimaryKeys(null, username.toUpperCase(), tableName.toUpperCase());
            if (primaryKeys.next()) {
                idColumnName = primaryKeys.getString("COLUMN_NAME");
            } else {
                // If no primary key, pick the first numeric column
                for (ColumnMetadata colMeta : columnMetadataList) {
                    if (colMeta.isNumeric()) {
                        idColumnName = colMeta.columnName;
                        break;
                    }
                }
            }

            if (idColumnName == null) {
                System.err.println("No suitable numeric ID column found for partitioning.");
                return;
            }

            // Get min and max ID values
            String minMaxQuery = "SELECT MIN(" + idColumnName + ") AS MIN_ID, MAX(" + idColumnName + ") AS MAX_ID FROM " + tableName;
            try (Statement stmt = oracleConnection.createStatement();
                 ResultSet rs = stmt.executeQuery(minMaxQuery)) {
                if (rs.next()) {
                    minId = rs.getLong("MIN_ID");
                    maxId = rs.getLong("MAX_ID");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        long totalRecords = maxId - minId + 1;
        long chunkSize = totalRecords / numThreads;

        // Build the dynamic TableDefinition
        TableDefinition tableDefinition = new TableDefinition(new TableName("Extract", tableName));

        for (ColumnMetadata colMeta : columnMetadataList) {
            SqlType sqlType = mapOracleTypeToHyper(colMeta);
            if (sqlType == null) {
                System.err.println("Unsupported Oracle type: " + colMeta.typeName + " for column " + colMeta.columnName);
                return;
            }
            tableDefinition.addColumn(colMeta.columnName, sqlType);
        }

        // Executor for reader threads
        ExecutorService readerExecutor = Executors.newFixedThreadPool(numThreads);
        List<Future<Void>> futures = new ArrayList<>();

        // Start the Hyper process and connection
        try (HyperProcess hyperProcess = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU);
             com.tableau.hyperapi.Connection hyperConnection = new com.tableau.hyperapi.Connection(
                     hyperProcess.getEndpoint(),
                     hyperFilePath,
                     CreateMode.CREATE_AND_REPLACE)) {

            // Create schema and table in the Hyper file
            hyperConnection.getCatalog().createSchema(new SchemaName("Extract"));
            hyperConnection.getCatalog().createTable(tableDefinition);

            // Start the writer thread
            Thread writerThread = new Thread(new WriterTask(hyperConnection, tableDefinition, queue, numThreads, columnMetadataList));
            writerThread.start();

            // Partition data ranges and submit reader tasks
            long currentStartId = minId;
            for (int i = 0; i < numThreads; i++) {
                long currentEndId = (i == numThreads - 1) ? maxId : (currentStartId + chunkSize - 1);

                ReaderTask task = new ReaderTask(
                        currentStartId,
                        currentEndId,
                        jdbcUrl,
                        username,
                        password,
                        queue,
                        tableName,
                        idColumnName,
                        columnMetadataList
                );
                futures.add(readerExecutor.submit(task));
                currentStartId = currentEndId + 1;
            }

            // Wait for all reader tasks to complete
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Shutdown reader executor
            readerExecutor.shutdown();

            // Wait for the writer thread to finish
            writerThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Data dump to Hyper file completed successfully.");
    }

    // ReaderTask class to read data from Oracle database
    static class ReaderTask implements Callable<Void> {
        private long startId;
        private long endId;
        private String jdbcUrl;
        private String username;
        private String password;
        private BlockingQueue<List<Object[]>> queue;
        private String tableName;
        private String idColumnName;
        private List<ColumnMetadata> columnMetadataList;

        public ReaderTask(long startId, long endId, String jdbcUrl, String username, String password,
                          BlockingQueue<List<Object[]>> queue, String tableName, String idColumnName,
                          List<ColumnMetadata> columnMetadataList) {
            this.startId = startId;
            this.endId = endId;
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.queue = queue;
            this.tableName = tableName;
            this.idColumnName = idColumnName;
            this.columnMetadataList = columnMetadataList;
        }

        @Override
        public Void call() {
            StringBuilder queryBuilder = new StringBuilder("SELECT ");
            for (int i = 0; i < columnMetadataList.size(); i++) {
                ColumnMetadata colMeta = columnMetadataList.get(i);
                queryBuilder.append(colMeta.columnName);
                if (i < columnMetadataList.size() - 1) {
                    queryBuilder.append(", ");
                }
            }
            queryBuilder.append(" FROM ").append(tableName)
                    .append(" WHERE ").append(idColumnName).append(" BETWEEN ? AND ?");

            String query = queryBuilder.toString();

            try (java.sql.Connection oracleConnection = DriverManager.getConnection(jdbcUrl, username, password);
                 PreparedStatement pstmt = oracleConnection.prepareStatement(query)) {

                pstmt.setLong(1, startId);
                pstmt.setLong(2, endId);
                pstmt.setFetchSize(10000); // Adjust fetch size for batch processing

                ResultSet rs = pstmt.executeQuery();
                List<Object[]> batch = new ArrayList<>();

                while (rs.next()) {
                    Object[] row = new Object[columnMetadataList.size()];
                    for (int i = 0; i < columnMetadataList.size(); i++) {
                        ColumnMetadata colMeta = columnMetadataList.get(i);
                        Object value = getValueFromResultSet(rs, colMeta, i + 1);
                        row[i] = value;
                    }

                    batch.add(row);

                    // Once batch size is reached, put it into the queue
                    if (batch.size() >= 10000) {
                        queue.put(new ArrayList<>(batch));
                        batch.clear();
                    }
                }

                // Put any remaining rows into the queue
                if (!batch.isEmpty()) {
                    queue.put(new ArrayList<>(batch));
                }

                // Signal completion by adding the POISON_PILL
                queue.put(POISON_PILL);

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    // WriterTask class to write data to the Hyper file
    static class WriterTask implements Runnable {
        private com.tableau.hyperapi.Connection hyperConnection;
        private TableDefinition tableDefinition;
        private BlockingQueue<List<Object[]>> queue;
        private int numReaderThreads;
        private List<ColumnMetadata> columnMetadataList;

        public WriterTask(com.tableau.hyperapi.Connection hyperConnection, TableDefinition tableDefinition,
                          BlockingQueue<List<Object[]>> queue, int numReaderThreads,
                          List<ColumnMetadata> columnMetadataList) {
            this.hyperConnection = hyperConnection;
            this.tableDefinition = tableDefinition;
            this.queue = queue;
            this.numReaderThreads = numReaderThreads;
            this.columnMetadataList = columnMetadataList;
        }

        @Override
        public void run() {
            int completedReaders = 0;
            try (Inserter inserter = new Inserter(hyperConnection, tableDefinition)) {
                while (completedReaders < numReaderThreads) {
                    List<Object[]> batch = queue.take();
                    if (batch == POISON_PILL) {
                        completedReaders++;
                    } else {
                        for (Object[] rowData : batch) {
                            for (int i = 0; i < rowData.length; i++) {
                                Object value = rowData[i];
                                ColumnMetadata colMeta = columnMetadataList.get(i);

                                // Handle null values
                                if (value == null) {
                                    inserter.addNull();
                                } else {
                                    // Call the appropriate inserter.add() method based on data type
                                    switch (colMeta.hyperType.getTag()) {
                                        case DOUBLE:
                                            inserter.add((Double) value);
                                            break;
                                        case BIG_INT:
                                            inserter.add((Long) value);
                                            break;
                                        case TEXT:
                                            inserter.add((String) value);
                                            break;
                                        case DATE:
                                            inserter.add((LocalDate) value);
                                            break;
                                        case TIMESTAMP:
                                            inserter.add((Instant) value);
                                            break;
                                        case BOOL:
                                            inserter.add((Boolean) value);
                                            break;
                                        case BYTES:
                                            inserter.add((byte[]) value);
                                            break;
                                        default:
                                            inserter.add(value.toString());
                                            break;
                                    }
                                }
                            }
                            inserter.endRow();
                        }
                    }
                }
                inserter.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Helper method to map Oracle data types to Hyper SqlType
    private static SqlType mapOracleTypeToHyper(ColumnMetadata colMeta) {
        SqlType sqlType;
        switch (colMeta.typeName) {
            case "NUMBER":
                if (colMeta.decimalDigits == 0) {
                    sqlType = SqlType.bigInt();
                } else {
                    sqlType = SqlType.doublePrecision();
                }
                break;
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "CLOB":
            case "NCLOB":
                sqlType = SqlType.text();
                break;
            case "DATE":
                sqlType = SqlType.date();
                break;
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                sqlType = SqlType.timestamp();
                break;
            case "BLOB":
            case "RAW":
            case "LONG RAW":
                sqlType = SqlType.bytes();
                break;
            default:
                sqlType = null; // Unsupported type
                break;
        }
        colMeta.hyperType = sqlType;
        return sqlType;
    }

    // Helper method to retrieve value from ResultSet based on column metadata
    private static Object getValueFromResultSet(ResultSet rs, ColumnMetadata colMeta, int columnIndex) throws SQLException {
        Object value;
        switch (colMeta.typeName) {
            case "NUMBER":
                if (colMeta.decimalDigits == 0) {
                    long longValue = rs.getLong(columnIndex);
                    value = rs.wasNull() ? null : longValue;
                } else {
                    double doubleValue = rs.getDouble(columnIndex);
                    value = rs.wasNull() ? null : doubleValue;
                }
                break;
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "CLOB":
            case "NCLOB":
                String str = rs.getString(columnIndex);
                value = rs.wasNull() ? null : str;
                break;
            case "DATE":
                java.sql.Date date = rs.getDate(columnIndex);
                value = rs.wasNull() ? null : date.toLocalDate();
                break;
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                Timestamp timestamp = rs.getTimestamp(columnIndex);
                value = rs.wasNull() ? null : timestamp.toInstant();
                break;
            case "BLOB":
            case "RAW":
            case "LONG RAW":
                byte[] bytes = rs.getBytes(columnIndex);
                value = rs.wasNull() ? null : bytes;
                break;
            default:
                value = rs.getObject(columnIndex);
                if (rs.wasNull()) {
                    value = null;
                }
                break;
        }
        return value;
    }

    // Class to hold column metadata
    static class ColumnMetadata {
        String columnName;
        int dataType;
        String typeName;
        int columnSize;
        int decimalDigits;
        boolean isNullable;
        SqlType hyperType;

        public ColumnMetadata(String columnName, int dataType, String typeName, int columnSize, int decimalDigits, boolean isNullable) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.isNullable = isNullable;
            this.hyperType = null;
        }

        public boolean isNumeric() {
            return "NUMBER".equals(typeName);
        }
    }
}
