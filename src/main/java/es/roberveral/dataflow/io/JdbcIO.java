package es.roberveral.dataflow.io;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableList;
import org.apache.metamodel.*;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.QuerySplitter;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JdbcIO {

    /**
     * Returns a {@link Source} builder configured with the given parameters.
     *
     * @param connectionString jdbc connection string.
     * @param driver           database driver name
     * @param user             username to access the database
     * @param password         password to access the database
     * @param query            query to read from
     * @return a {@link Source} object
     * @throws Exception if the driver cannot be found or the connection cannot be made
     */
    public static Source source(String connectionString, String driver, String user, String password, String query) throws Exception {
        return new Source(connectionString, driver, user, password, query);
    }

    /**
     * Returns a {@link Sink} builder configured with the given parameters
     * @param connectionString jdbc connection string.
     * @param driver database driver name
     * @param user username to access the database
     * @param password password to access the database
     * @param tableName name to insert into
     * @param fieldNames map of key-value pairs with a column name and a {@link ColumnType} associated (for creating
     *                   the table if not exists)
     * @return a {@link Sink} object.
     */
    public static Sink sink(String connectionString, String driver, String user, String password, String tableName, Map<String, ColumnType> fieldNames) {
        return new Sink(connectionString, driver, user, password, tableName, fieldNames);
    }

    /**
     * Class that represents a row in a JDBC query.
     * It allows to get all the fields by the column index.
     *
     * @author Roberto Veral
     */
    @DefaultCoder(AvroCoder.class)
    public static class Record implements Serializable {
        private List<Object> data;

        public Record() {
        }

        /**
         * Obtains the number of columns in the row
         * @return number of columns of the record
         */
        public int getNumCols() {
            return data.size();
        }

        /**
         * Creates a new {@link Record}, taking each item of the data list as a field of the query row.
         *
         * @param data input fields of the query row.
         */
        public Record(List<Object> data) {
            // Each item of the input list is a field of the record
            this.data = new ArrayList<Object>(data);
        }

        /**
         * Obtains col field of the record as a Object.
         * It can throw an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Object getObject(int col) {
            // Returns col field of the record as a object
            return data.get(col);
        }

        /**
         * Obtains col field of the record as a String.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to String.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public String getString(int col) {
            // Returns col field of the record as a String, throwing an exception
            // if it can't be casted
            return (String) data.get(col);
        }

        /**
         * Obtains col field of the record as a Integer.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Integer.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Integer getInteger(int col) {
            // Returns col field of the record as a Integer, throwing an exception
            // if it can't be casted
            return (Integer) data.get(col);
        }

        /**
         * Obtains col field of the record as a Double.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Double.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Double getDouble(int col) {
            // Returns col field of the record as a Double, throwing an exception
            // if it can't be casted
            return (Double) data.get(col);
        }

        /**
         * Obtains col field of the record as a Float.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Float.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Float getFloat(int col) {
            // Returns col field of the record as a Float, throwing an exception
            // if it can't be casted
            return (Float) data.get(col);
        }

        /**
         * Obtains col field of the record as a Long.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Long.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Long getLong(int col) {
            // Returns col field of the record as a Long, throwing an exception
            // if it can't be casted
            return (Long) data.get(col);
        }

        /**
         * Obtains col field of the record as a Short.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Short.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Short getShort(int col) {
            // Returns col field of the record as a Short, throwing an exception
            // if it can't be casted
            return (Short) data.get(col);
        }

        /**
         * Obtains col field of the record as a Date.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Date.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Date getDate(int col) {
            // Returns col field of the record as a Date, throwing an exception
            // if it can't be casted
            return (Date) data.get(col);
        }

        /**
         * Obtains col field of the record as a Timestamp.
         * It can thow an ArrayIndexOutOfBoundsException if col is lower than 0 or greater
         * than the number of fields of the record, or an ClassCastException if the field cannot
         * be casted to Timestamp.
         *
         * @param col column to obtain.
         * @return row field.
         */
        public Timestamp getTimestamp(int col) {
            // Returns col field of the record as a Timestamp, throwing an exception
            // if it can't be casted
            return (Timestamp) data.get(col);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /**
     * A {@link com.google.cloud.dataflow.sdk.io.Source} that reads the result of a Jdbc query
     * as {@code Record} objects.
     *
     * @author Roberto Veral
     */
    public static class Source extends BoundedSource<Record> {
        private final String connectionString;
        private final String driver;
        private final String user;
        private final String password;
        private final String query;

        /**
         * Creates a new {@link Source} taking the data needed to create a database connection
         *
         * @param connectionString jdbc connection string.
         * @param driver           database driver name
         * @param user             username to access the database
         * @param password         password to access the database
         * @param query            query to read from
         */
        public Source(String connectionString, String driver, String user, String password, String query) {
            this.connectionString = connectionString;
            this.driver = driver;
            this.user = user;
            this.password = password;
            this.query = query;
        }

        /**
         * Obtains the size in bytes of a row of the query associated with the {@link Source}
         *
         * @return size in bytes of a row
         */
        private long getSizeOfRow() throws Exception {
            /* Navigate across metadata to obtain the size of a row */
            long sizeOfRow = 0;
            Class.forName(driver);
            Query q = new JdbcDataContext(DriverManager.getConnection(connectionString, user, password)).parseQuery(query);
            for (SelectItem col : q.getSelectClause().getItems()) {
                sizeOfRow += col.getColumn().getColumnSize();
            }
            return sizeOfRow;
        }

        @Override
        public List<? extends BoundedSource<Record>> splitIntoBundles(long l, PipelineOptions pipelineOptions) throws Exception {
            /* Obtains estimated size for the query */
            long estimatedSize = getEstimatedSizeBytes(pipelineOptions);
            /* Obtains the number of splits based on the desired size in bytes */
            long numSplits = Math.round(estimatedSize / l);
            /* If the splits to generate is 1, the same Source is returned */
            if (numSplits <= 1) {
                return ImmutableList.of(this);
            } else {
                /* Initializes metadata for the query */
                Class.forName(driver);
                DataContext connection = new JdbcDataContext(DriverManager.getConnection(connectionString, user, password));
                Query query = connection.parseQuery(this.query);
                /* Creates a inmutable list builder */
                ImmutableList.Builder<Source> splits = ImmutableList.builder();
                /* Calculates the number of rows for each query */
                long rowsPerSplit = (estimatedSize / getSizeOfRow()) / numSplits;
                /* Split the query into smaller queries */
                QuerySplitter qs = new QuerySplitter(connection, query);
                qs.setMaxRows(rowsPerSplit);
                /* Generate one source per split query */
                for (Query q : qs.splitQuery()) {
                    System.out.println(q.toSql());
                    splits.add(new Source(connectionString, driver, user, password, q.toSql()));
                }
                /* Returns the source list */
                return splits.build();
            }
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
            /* Initializes query metadata */
            Class.forName(driver);
            DataContext connection = new JdbcDataContext(DriverManager.getConnection(connectionString, user, password));
            Query query = connection.parseQuery("SELECT COUNT(*) FROM (" + this.query + ") AUX");
            /* Obtains the number of rows of the result */
            DataSet conteo = connection.executeQuery(query.selectCount());
            conteo.next();
            long numRows = (long) conteo.getRow().getValue(0);
            conteo.close();
            /* Navigate across metadata to obtain the size of a row */
            long sizeOfRow = getSizeOfRow();
            /* The final size is the number of rows multiplied by the size of each row */
            return numRows * sizeOfRow;
        }

        @Override
        public boolean producesSortedKeys(PipelineOptions pipelineOptions) throws Exception {
            return false;
        }

        @Override
        public BoundedReader<Record> createReader(PipelineOptions pipelineOptions) throws IOException {
            return new JdbcReader(this);
        }

        @Override
        public void validate() {
            Preconditions.checkNotNull(connectionString, "connectionString");
            Preconditions.checkNotNull(driver, "driver");
            Preconditions.checkNotNull(user, "user");
            Preconditions.checkNotNull(password, "password");
            Preconditions.checkNotNull(query, "query");
        }

        @Override
        public Coder<Record> getDefaultOutputCoder() {
            return AvroCoder.of(Record.class);
        }
    }

    /**
     * A {@link BoundedSource.BoundedReader} over the records of a Jdbc query.
     *
     * @author Roberto Veral
     */
    public static class JdbcReader extends BoundedSource.BoundedReader<Record> {
        private final Source source;
        private DataSet results;

        /**
         * Creates a {@link JdbcReader} for the given {@link Source}
         *
         * @param source source object.
         */
        public JdbcReader(Source source) {
            this.source = source;
        }

        @Override
        public boolean start() throws IOException {
            try {
                /* Initializes query metadata */
                Class.forName(source.driver);
                DataContext connection = new JdbcDataContext(DriverManager.getConnection(source.connectionString, source.user, source.password));
                Query query = connection.parseQuery(source.query);
                /* Execute the query and saves the Result set */
                results = connection.executeQuery(query);
                /* Set the pointer on the first result */
                return results.next();
            } catch (Exception e) {
                throw new IOException("Exception accesing the results: " + e);
            }
        }

        @Override
        public boolean advance() throws IOException {
            /* Advance to the next row */
            return results.next();
        }

        @Override
        public Record getCurrent() throws NoSuchElementException {
            Row row = results.getRow();
            /* Return a Record object with the row columns */
            return new Record(Arrays.asList(row.getValues()));
        }

        @Override
        public void close() throws IOException {
            results.close();
        }

        @Override
        public BoundedSource<Record> getCurrentSource() {
            return source;
        }
    }

    /**
     * A {@link com.google.cloud.dataflow.sdk.io.Sink} that writes a {@link com.google.cloud.dataflow.sdk.values.PCollection}
     * of {@link Record} to a Jdbc database.
     *
     * @author Roberto Veral
     */
    public static class Sink extends com.google.cloud.dataflow.sdk.io.Sink<Record> {
        private final String connectionString;
        private final String driver;
        private final String user;
        private final String password;
        private final String tableName;
        private final Map<String, ColumnType> fieldNames;

        /**
         * Creates a {@link Sink} for the given database.
         *
         * @param connectionString jdbc connection strinb
         * @param driver driver class name
         * @param user user to access the database
         * @param password password to access the database
         * @param tableName name of the target table
         * @param fieldNames map of key-value pairs with a column name and a {@link ColumnType} associated (for creating
         *                   the table if not exists).
         */
        public Sink(String connectionString, String driver, String user, String password, String tableName, Map<String, ColumnType> fieldNames) {
            this.connectionString = connectionString;
            this.driver = driver;
            this.user = user;
            this.password = password;
            this.tableName = tableName;
            this.fieldNames = fieldNames;
        }

        @Override
        public void validate(PipelineOptions options) {
            Preconditions.checkNotNull(connectionString, "connectionString");
            Preconditions.checkNotNull(driver, "driver");
            Preconditions.checkNotNull(user, "user");
            Preconditions.checkNotNull(password, "password");
            Preconditions.checkNotNull(tableName, "tableName");
            Preconditions.checkNotNull(fieldNames, "fieldNames");
        }

        @Override
        public WriteOperation<Record, ?> createWriteOperation(PipelineOptions options) {
            return new JdbcWriteOperation(this);
        }
    }

    /**
     * A {@link com.google.cloud.dataflow.sdk.io.Sink.WriteOperation} for Jdbc {@link Sink}
     *
     * @author Roberto Veral
     */
    public static class JdbcWriteOperation extends com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<Record, Boolean> {
        private final Sink sink;

        /**
         * Creates the {@link JdbcWriteOperation} for the given {@link Sink}
         * @param sink target to write in.
         */
        public JdbcWriteOperation(Sink sink) {
            this.sink = sink;
        }

        @Override
        public void initialize(PipelineOptions options) throws Exception {
            /* Discover if table exists */
            Class.forName(sink.driver);
            UpdateableDataContext connection = new JdbcDataContext(DriverManager.getConnection(sink.connectionString, sink.user, sink.password));
            Table table = connection.getTableByQualifiedLabel(sink.tableName);
            if (table == null) {
                /* Create table if not exists */
                connection.executeUpdate(updateCallback -> {
                    TableCreationBuilder newTable = updateCallback.createTable(connection.getDefaultSchema(), sink.tableName);
                    /* Table is build based on the field names and types given */
                    for (Map.Entry<String, ColumnType> e : sink.fieldNames.entrySet()) {
                        newTable = newTable.withColumn(e.getKey()).ofType(e.getValue());
                    }
                    newTable.execute();
                });
            }
        }

        @Override
        public void finalize(Iterable<Boolean> writerResults, PipelineOptions options) throws Exception {
            // Nothing to do here
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.Writer<Record, Boolean> createWriter(PipelineOptions options) throws Exception {
            return new JdbcWriter(this);
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink<Record> getSink() {
            return sink;
        }
    }

    /**
     * A {@link com.google.cloud.dataflow.sdk.io.Sink.Writer} for a {@link Sink}.
     * Writes the result set to the relational database target indicated in the Sink.
     *
     * @author Roberto Veral
     */
    public static class JdbcWriter extends com.google.cloud.dataflow.sdk.io.Sink.Writer<Record, Boolean> {
        private final JdbcWriteOperation writeOp;

        private List<Record> writeBuffer;

        /**
         * Creates a {@link JdbcWriter} for the given {@link JdbcWriteOperation}
         * @param writeOp write operation for the target Sink
         */
        public JdbcWriter(JdbcWriteOperation writeOp) {
            this.writeOp = writeOp;
        }

        @Override
        public void open(String uId) throws Exception {
            this.writeBuffer = new ArrayList<>();
        }

        @Override
        public void write(Record value) throws Exception {
            writeBuffer.add(value);
        }

        @Override
        public Boolean close() throws Exception {
            if (writeBuffer.size() > 0) {
                /* Saves the results in batch mode to the target table */
                Class.forName(writeOp.sink.driver);
                UpdateableDataContext connection = new JdbcDataContext(DriverManager.getConnection(writeOp.sink.connectionString, writeOp.sink.user, writeOp.sink.password));
                Table table = connection.getTableByQualifiedLabel(writeOp.sink.tableName);
                try {
                    connection.executeUpdate(new BatchUpdateScript() {
                        @Override
                        public void run(UpdateCallback updateCallback) {
                        /* Generates insert for each Record */
                            for (Record r : writeBuffer) {
                                RowInsertionBuilder builder = updateCallback.insertInto(table);
                                for (int i = 0; i < r.getNumCols(); i++) {
                                    builder = builder.value(table.getColumn(0), r.getObject(0));
                                }
                                builder.execute();
                            }
                        }
                    });
                    return true;
                } catch (MetaModelException e) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<Record, Boolean> getWriteOperation() {
            return writeOp;
        }
    }
}
