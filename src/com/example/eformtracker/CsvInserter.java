package com.example.eformtracker;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Date;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class CsvInserter {
	private static final String PROJECT_DIR = System.getProperty("user.dir");
	private Database db;
	private String table;
	private ValueMap[] valueMap;
	
	
	
	CsvInserter(Database db, String filename) throws SQLException {
		this.db = db;
		this.table = filename.toLowerCase().replaceFirst("\\..*(?!\\.)", ""); // Assumes filename without extension = table name
		this.valueMap = getAttributes();
	}
	
	
	// Returns all attributes for the currently selected table
	private ValueMap[] getAttributes() throws SQLException {
		List<ValueMap> attributes = new ArrayList<>();
		Statement statement = db.connection.createStatement();
		ResultSet results = statement.executeQuery("DESCRIBE " + table);
		
		while (results.next()) {
			attributes.add(new ValueMap(results.getString("Field"), results.getString("Type"), results.getRow()));
		}
		
		ValueMap[] valMap = new ValueMap[attributes.size()];
		for (int i = 0; i < valMap.length; ++i) {
			valMap[i] = attributes.get(i);
		}
		
		return valMap;
	}
	
	// Construct an INSERT statement from a set of key-value pairs
	public String buildInsert() throws SQLException {
		StringBuilder insert = new StringBuilder("INSERT INTO " + table + "(");
		StringBuilder placeholder = new StringBuilder(" VALUES (");
		
		int counter = 0;
		String punct = null;
		for (ValueMap value : valueMap) {
				punct = (++counter < valueMap.length) ? ", " : ")";
				insert.append("`" + value.field + "`" + punct);
				placeholder.append("?" + punct);
		}
		
		return (insert.toString() + placeholder.toString());
	}
	
	
	// Set values for row attributes based on CSV
	public void setValue(PreparedStatement preparedStatement, CSVRecord record, ValueMap valMap) throws NumberFormatException, SQLException {
		String type = valMap.type.replaceAll("\\(.*\\)", "");
		
		switch (type) {
		case "int": {
			preparedStatement.setInt(valMap.columnNumber, Integer.parseInt(record.get(valMap.field)));
			break;
		}
		case "varchar": {
			preparedStatement.setString(valMap.columnNumber,  record.get(valMap.field));
			break;
		}
		case "timestamp": {
			DateTimeFormatter timestamp = DateTimeFormatter.ofPattern("M[M]/d[d]/yyyy H[H]:mm");
			preparedStatement.setTimestamp(valMap.columnNumber, Timestamp.valueOf(LocalDateTime.parse(record.get("Sent On"), timestamp)));
			break;
		}
		case "enum": {
			preparedStatement.setString(valMap.columnNumber, record.get(valMap.field));
			break;
		}
		case "date": {
			DateTimeFormatter date = DateTimeFormatter.ofPattern("M[M]/d[d]/yy[yy]");
			preparedStatement.setDate(valMap.columnNumber, Date.valueOf(LocalDate.parse(record.get(valMap.field), date)));
			break;
		}
		default:
			System.out.println("Setting value for type \"" + valMap.type + "\" is not supperted!");
		}
	}
	
	public void insertRecords() throws IOException, NumberFormatException, SQLException {
		// Create prepared statement
		PreparedStatement preparedStatement = db.connection.prepareStatement(buildInsert(), Statement.RETURN_GENERATED_KEYS);
		
		// Open CSV for parsing
		System.out.println("Parsing CSV...");
		Reader in = new FileReader(new File(PROJECT_DIR, table + ".csv"));
		
		// Iterate through records and add batch statements
		Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
		Map<String, String> insertedRecords = new HashMap<>();
		
		for (CSVRecord record : records) {
			for (ValueMap val : valueMap) {
				setValue(preparedStatement, record, val);
			}
			preparedStatement.addBatch();
		}
		
		in.close();
		
		// Execute batch statement
		System.out.println("Inserting records into " + table + " table");
		int[] count = preparedStatement.executeBatch();
		
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
	}
	
	
	// Class for holding fields necessary to build statement
	private static class ValueMap {
		public String field;
		public String type;
		public int columnNumber;
		
		ValueMap(String field, String type, int columnNumber) {
			this.field = field;
			this.type = type;
			this.columnNumber = columnNumber;
		}
	}
}
