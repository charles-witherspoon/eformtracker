package com.example.eformtracker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Database {

	private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String URL = "jdbc:mysql://localhost:3306/eform_tracker";
	private static final String USER = "user_test";
	private static final String PASSWORD = "passwd_test";
	private static final String PROJECT_DIR = System.getProperty("user.dir");
	private static Database singletonDatabase;
	private PreparedStatement preparedStatement;
	private String tables[];
	public Connection connection;
	
	private Database() throws ClassNotFoundException, SQLException {
		// Register driver
		Class.forName(DRIVER);
		
		// Open connection
		connection = DriverManager.getConnection(URL, USER, PASSWORD);
		
		// Get tables
		tables = getTables();
	}
	
	// Return singleton instance of a database
	public static Database getDatabase() throws ClassNotFoundException, SQLException {
		if (singletonDatabase == null) {
			singletonDatabase = new Database();
		}
		return singletonDatabase;
	}
	
	
	// Get tables in database
	private String[] getTables() throws SQLException {
		List<String> tableList = new ArrayList<>();
		String[] types = {"TABLE"};
		ResultSet results = 
				connection.getMetaData().getTables(null, null, "%", types);
		
		while (results.next()) {
			tableList.add(results.getString("TABLE_NAME"));
		}
		
		String[] tableArr = new String[tableList.size()];
		
		for (int i = 0; i < tableArr.length; ++i) {
			tableArr[i] = tableList.get(i);
		}
		
		return tableArr;
	}
	
	public void prepareStatement(String sqlQuery) throws SQLException {
		connection.setAutoCommit(false);
		preparedStatement = connection.prepareStatement(sqlQuery);
	}
	
	public ResultSet executeStatement() throws SQLException {
		return preparedStatement.executeQuery();
	}
	
	// Close database connection
	public void close() throws SQLException {
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		connection.close();
	}
}
