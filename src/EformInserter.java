import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;

public class EformInserter {

	private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String URL = "jdbc:mysql://localhost:3306/iser_tracker";
	private static final String USER = "user_test";
	private static final String PASSWORD = "passwd_test";
	private static final String PROJECT_DIR = System.getProperty("user.dir");
	
	public static void main(String[] args) {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			// Register driver
			Class.forName(DRIVER);

			// Open connection
			System.out.println("Opening connection to database...");
			conn = DriverManager.getConnection(URL, USER, PASSWORD);

			// Build INSERT statement
			String insert = "INSERT INTO eform "
					+ "(eform, pri, seq, queue, added_by, sent_by, sent_on, summary, workflow, changed_on, changed_by, need_by)"
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			
			// Create prepared statement
			stmt = conn.prepareStatement(insert);
			conn.setAutoCommit(false);
				
			// Open CSV for parsing
			Reader in = new FileReader(new File(PROJECT_DIR, "eforms.csv"));
//			Reader in = new FileReader(new File(PROJECT_DIR, "eforms_with_duplicates.csv")); // Test insert error
 
			Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
			
			// Build statement and add it batch
			DateTimeFormatter timestamp = DateTimeFormatter.ofPattern("M[M]/d[d]/yyyy"
					+ " H[H]:mm");
			DateTimeFormatter date = DateTimeFormatter.ofPattern("M[M]/d[d]/yy[yy]");
			
			ArrayList<Pair<String, String>> insertedRecords = new ArrayList<>(); // Store list of records
			
			System.out.println("Building INSERT statement from CSV Records...");
			for (CSVRecord record : records) {
				stmt.setInt(1, Integer.parseInt(record.get("Eform")));
				stmt.setInt(2, Integer.parseInt(record.get("PRI")));
				stmt.setInt(3, Integer.parseInt(record.get("Seq")));
				stmt.setString(4, record.get("Queue"));
				stmt.setString(5, record.get("Added By"));
				stmt.setString(6, record.get("Sent By"));
				stmt.setTimestamp(7, 
						Timestamp.valueOf(LocalDateTime.parse(record.get("Sent On"), timestamp)));
				stmt.setString(8, record.get("Summary"));
				stmt.setString(9, record.get("Workflow"));
				stmt.setTimestamp(10, 
						Timestamp.valueOf(LocalDateTime.parse(record.get("Changed On"), timestamp)));
				stmt.setString(11, record.get("Changed By"));
				stmt.setDate(12, Date.valueOf(LocalDate.parse(record.get("Need By"), date)));
				stmt.addBatch();
				insertedRecords.add(Pair.of(record.get("Eform"), record.get("Summary")));
			}
			
			in.close();
			
			// Execute batch statement
			System.out.println("Inserting records into 'iser' table...");
			int[] count = stmt.executeBatch();
			System.out.println("Insert complete!");
			
			String pk = null;
			String summary = null;
			// Dump inserted records to screen
			
			System.out.println("\nRecords Inserted: " + count.length + "\n\n"
					+ "|Primary Key\t|Summary\n"
					+ "|-------------------------------------------------------");
			for (int i = 0; i < count.length; ++i) {
				if (count[i] != Statement.EXECUTE_FAILED) {
					pk = insertedRecords.get(i).getLeft();
					summary = insertedRecords.get(i).getRight();
					System.out.println("|" + pk + "\t|" + summary);
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (BatchUpdateException be) {
			// print number of failures
			int[] results = be.getUpdateCounts();
			int failures = 0;
			
			for (int result: results) {
				if (result == Statement.EXECUTE_FAILED) {
					++failures;
				}	
			}
			System.out.println("Error: Could not insert "+ failures + " rows");
			System.out.println("\t" + be.getCause());
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// CSV Reader
			System.out.println("Error: File not found. Please check filepath.");
		} catch (IOException e) {
			// CSV Reader
			e.printStackTrace();
		} finally {
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
