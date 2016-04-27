package com.tinamica.dataflow.transforms;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.reflect.Nullable;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Class that represents a row in a JDBC query.
 * It allows to get all the fields by the column index.
 * 
 * @author Roberto Veral
 */
class Record {
	private List<Object> data;
	
	/**
	 * Creates a new Record, taking each item of the data list as a field of the query row.
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
}

/**
 * Class that represents a JDBC connection. It contains the database connection info and
 * the query to be executed, and it can perform the execution of the query.
 * 
 * @author Roberto Veral
 *
 */
@DefaultCoder(AvroCoder.class)
class JDBCQuery implements Serializable {
	private static final long serialVersionUID = 1L;
	@Nullable private String connectionString;
	@Nullable private String driver;
	@Nullable private String user;
	@Nullable private String password;
	@Nullable private String query;
	
	/**
	 * Empty constructor without initialization
	 */
	public JDBCQuery() {}
	
	/**
	 * Creates the object and initializes all its properties
	 * @param connectionString connection string to access to the database in which the query has to be executed.
	 * @param driver name of the JDBC driver class for the database.
	 * @param user username to access to the database.
	 * @param password password to access to the database.
	 * @param query query that has to be executed in the database.
	 */
	public JDBCQuery(String connectionString, String driver, String user, String password, String query) {
		this.connectionString = connectionString;
		this.driver = driver;
		this.user = user;
		this.password = password;
		this.query = query;
	}
	
	/**
	 * Retrieves the query results from the database.
	 * Each row of the result set is transformed into a Record object.
	 * @return list of records of the result set
	 * @throws Exception It can throw an exception if the query execution goes wrong because
	 * of a bad parameter used, or a missing driver class.
	 */
	public List<Record> execute() throws Exception {
		// Initialization of connection variables
		Connection c = null;
		Statement stmt = null;
		// Initialization of result variable
		ArrayList<Record> results = new ArrayList<Record>();
		// Loads driver class
		Class.forName(driver);
		// Gets a connection to the database
		c = DriverManager.getConnection(connectionString, user, password);
		// Set all the queries in the same transaction
		c.setAutoCommit(false);
		// Creates a statement an executes the query
		stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		// Iterate over the query result set
		while(rs.next()) {
			// Generate a list of all the fields
			ArrayList<Object> fields = new ArrayList<Object>();
			// Iterate over all the columns
			for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				// Add the field to the fields list
				fields.add(rs.getObject(i));
			}
			// Add the Record to the results list
			results.add(new Record(fields));
		}
		// Closes the connection to the database
		rs.close();
		stmt.close();
		c.close();
		// Return the results list
		return results;
	}
}

/**
 * A {@link PTransform} that performs a {@link JDBCRead} on a input {@link PCollection}
 * of {@link JDBCQuery} elements. A {@link JDBCRead} performs the execution of the
 * queries received in the input collection, and returns a {@link PCollection} of 
 * {@link Record} elements with all the rows in the queries received. 
 * 
 * Be carefull! Each query of the input {@link PCollection} will be executed in ONE node, so
 * results of the query must fit in memory in one node. 
 * 
 * @author Roberto Veral
 *
 */
public class JDBCRead extends PTransform<PCollection<JDBCQuery>, PCollection<Record>> {
	private static final long serialVersionUID = 1L;
	
  /**
   * Returns a {@code JDBCRead} {@code PTransform}.
   */
	public static JDBCRead create() {
		return new JDBCRead();
	}
	
	@Override
	public PCollection<Record> apply(PCollection<JDBCQuery> queries) {
		// Read is performed in two steps:
		// First, for each query in the input collection, it is executed and 
		// processed the results, being flattened.
		// Second, the elements are grouped by row number to repartition the data
		// across the nodes
		return queries.apply(ParDo.named("ReadQueryResults")
				.of(new DoFn<JDBCQuery, KV<Long, Record>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(DoFn<JDBCQuery, KV<Long, Record>>.ProcessContext arg0) throws Exception {
						long i = 0;
						// For each result, a new output is generated, adding the row number as key
						for (Record record : arg0.element().execute()) {
							arg0.output(KV.of(i, record));
							i++;
						}
					}
				})).apply(new GroupByKey.GroupByKeyOnly<Long, Record>())
				.apply(ParDo.named("Repartition").of(new DoFn<KV<Long, Iterable<Record>>, Record>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(DoFn<KV<Long, Iterable<Record>>, Record>.ProcessContext arg0) throws Exception {
						/* For each record in the Key-Value pair, only the records are returned */
						for (Record record : arg0.element().getValue()) {
							arg0.output(record);
						}
						
					}
				}));
	}
}
