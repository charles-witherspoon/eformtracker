import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class EformInserter {

	private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String URL = "jdbc:mysql://localhost:3306/eform_tracker";
	private static final String USER = "user_test";
	private static final String PASSWORD = "passwd_test";
	private static final String PROJECT_DIR = System.getProperty("user.dir");

	
	// Class for holding fields necessary to build statement
	private static class ValueMap {
		public String tblName;
		public String field;
		public String type;
		public int tblColumn;
		
		ValueMap(String tblName, String field, String type, int tblColumn) {
			this.tblName = tblName; 
			this.field = field;
			this.type = type;
			this.tblColumn = tblColumn;
		}
		
		public String toString() {
			return String.format("Table Name: %s\tField: %s\tType: %s\tColumn Number: %s",
					tblName, field, type, tblColumn);
		}
	}
	
	
	
	// Takes in DatabaseMetaData object and returns the tables in current database
	public static String[] getTables(DatabaseMetaData dbmd) throws SQLException {
		ArrayList<String> tables = new ArrayList<>();
		String[] types = { "TABLE" };
		ResultSet results = dbmd.getTables(null, null, "%", types);
		while (results.next()) {
			tables.add(results.getString("TABLE_NAME"));
		}

		String[] tblArr = new String[tables.size()];

		for (int i = 0; i < tables.size(); ++i) {
			tblArr[i] = tables.get(i);
		}

		return tblArr;
	}

	
	// Takes in a Connection object and a table name and returns all attributes and types for the table
	public static ValueMap[] getAttributes(Connection conn, String tblName) throws SQLException {
		ArrayList<ValueMap> attrs = new ArrayList<>();
		
		Statement stmt = conn.createStatement();
		ResultSet res = stmt.executeQuery("DESCRIBE " + tblName);
		while (res.next()) {
			attrs.add(new ValueMap(tblName, res.getString("Field"), res.getString("Type"), res.getRow()));
		}
		
		ValueMap[] valMap = new ValueMap[attrs.size()];
		for (int i = 0; i < attrs.size(); ++i) {
			valMap[i] = attrs.get(i);
		}
		
		
		stmt.close();
		
		return valMap;
	}

	
	public static String buildInsert(ValueMap[] values) {
		StringBuilder insert = new StringBuilder("INSERT INTO " + values[0].tblName + "(");
		StringBuilder placehldr = new StringBuilder(" VALUES (");
		
		int counter = 0;
		String punc = "";
		for (ValueMap valMap : values) {
			if (++counter < values.length) {
				punc = ", ";
			} else {
				punc = ")";
			}
			insert.append("`" + valMap.field + "`" + punc);
			placehldr.append("?" + punc);
		}
		
		System.out.println(insert.toString() + placehldr.toString());
		
		return (insert.toString() + placehldr.toString());
	}
	
	
	public static void setValue(PreparedStatement stmt, CSVRecord record, ValueMap valMap) throws SQLException {
		
		switch (valMap.type.replaceAll("\\(.*\\)", "")) {
		
		case "int": {
			stmt.setInt(valMap.tblColumn, Integer.parseInt(record.get(valMap.field)));
			break;
		}
		
		case "varchar": {
			stmt.setString(valMap.tblColumn, record.get(valMap.field));
			break;
		}
		
		case "timestamp": {
			DateTimeFormatter timestamp = DateTimeFormatter.ofPattern("M[M]/d[d]/yyyy H[H]:mm");
			stmt.setTimestamp(valMap.tblColumn, Timestamp.valueOf(LocalDateTime.parse(record.get("Sent On"), timestamp)));
			break;
		}
		
		case "enum": {
			stmt.setString(valMap.tblColumn, record.get(valMap.field));
			break;
		}
		
		case "date": {
			DateTimeFormatter date = DateTimeFormatter.ofPattern("M[M]/d[d]/yy[yy]");
			stmt.setDate(valMap.tblColumn, Date.valueOf(LocalDate.parse(record.get(valMap.field), date)));
			break;
		}
		default:
			System.out.println("setValue for type \"" + valMap.type + "\" not supported");
		}
				
	}

	
	public static void main(String[] args) {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			// Register driver
			Class.forName(DRIVER);

			// Open connection
			System.out.println("Opening connection to database...");
			conn = DriverManager.getConnection(URL, USER, PASSWORD);

			
			// Get tables
			DatabaseMetaData dbmd = conn.getMetaData();
			String[] tblNames = getTables(dbmd);
			
			
			//TODO add support for multiple tables 	
			// Get table attributes
//			for (String tblName : tblNames) {
//			}
			
			ValueMap[] valMap = getAttributes(conn, tblNames[0]);
			// for table in tables if name == filename.replaceAll("\\.csv", "");
			
					
			
			// Build INSERT statement
			String insert = buildInsert(valMap);
			

			// Create prepared statement
			stmt = conn.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS);
			conn.setAutoCommit(false);

			
			// Open CSV for parsing
			Reader in = new FileReader(new File(PROJECT_DIR, "eform.csv"));
//			Reader in = new FileReader(new File(PROJECT_DIR, "eforms_with_duplicates.csv")); // Test insert error
 
			// Iterate through records and add batch statements
			Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
			HashMap<String, String> insertedRecords = new HashMap<>();
			
			for (CSVRecord record: records) {
				for (ValueMap val : valMap) {
					setValue(stmt, record, val);
					if (val.field.equals("Summary")) {
						insertedRecords.put(record.get("Eform"), record.get("Summary"));
					}
				}
				stmt.addBatch();
			}
			
			in.close();
			
			// Execute batch statement
			System.out.println("Inserting records into 'iser' table...");
			
			int[] count = stmt.executeBatch();

			
			// Currently keys are not auto-generated
//			ResultSet results = stmt.getGeneratedKeys();
			
			
			System.out.println("Insert complete!");
			
			String pk = null;
			String summary = null;
			// Dump inserted records to screen
			
			System.out.println("\nRecords Inserted: " + count.length + "\n\n"
					+ "|Primary Key\t|Summary\n"
					+ "|-------------------------------------------------------");
			
			int index = 0;
			for (Entry<String, String> entry : insertedRecords.entrySet()) {
				if (count[index] != Statement.EXECUTE_FAILED) {
					pk = entry.getKey();
					summary = entry.getValue();
					System.out.println("|" + pk + "\t|" + summary);
					++index;
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (BatchUpdateException be) {
			// print number of failures
			int[] results = be.getUpdateCounts();
			int failures = 0;

			for (int result : results) {
				if (result == Statement.EXECUTE_FAILED) {
					++failures;
				}
			}
			System.out.println("Error: Could not insert " + failures + " rows");
			System.out.println("\t" + be.getCause());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		catch (FileNotFoundException e) {
			// CSV Reader
			System.out.println("Error: File not found. Please check filepath.");
		} catch (IOException e) {
			// CSV Reader
			e.printStackTrace();
		} 
		finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				// Statement close error
				System.out.println("Error closing statement: " + e.toString());
			}

			try {
				if (conn != null) {
					Scanner in = new Scanner(System.in);
					System.out.println("\nCommit changes? Y/N");
					String userInput = in.next();

					while (!userInput.matches("[yn]|yes|no")) {
						System.out.println("Please enter Y/N");
						userInput = in.next();
					}

					if (userInput.toLowerCase().matches("y|(yes)")) {
						conn.commit();
					}

					in.close();
					System.out.println("Closing connection...");
					conn.close();
				}
			} catch (SQLException e) {
				// Connection close error
				System.out.println("Error closing connection: " + e.toString());
			}

			System.out.println("Goodbye!");
		}

	}

}
