package com.example.eformtracker;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class RequestHandler implements HttpHandler {

	private Database db;

	RequestHandler(Database db) {
		this.db = db;
	}

	@Override
	public void handle(HttpExchange httpExchange) throws IOException {
		if ("GET".equals(httpExchange.getRequestMethod())) {
			try {
				handleGetRequest(httpExchange);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if ("POST".equals(httpExchange.getRequestMethod())) {
			handlePostRequest(httpExchange);
		}
	}

	// Parse query for GET request and return data from database
	private String handleGetRequest(HttpExchange httpExchange) throws SQLException, IOException {
		OutputStream outputStream = httpExchange.getResponseBody();
		String htmlQuery = httpExchange.getRequestURI().getQuery();
		String sqlQuery = buildSelectQuery(htmlQuery);
		db.prepareStatement(sqlQuery);

		ResultSet results = db.executeStatement();
		ResultSetMetaData resMetaData = results.getMetaData();
		int columnCount = resMetaData.getColumnCount();
		StringBuilder records = new StringBuilder("{ \"data\": [");

		while (results.next()) {
			records.append("{");
			for (int i = 1; i <= columnCount; ++i) {
				records.append("\"" + resMetaData.getColumnName(i) + "\"" + ":")
						.append("\"" + results.getString(resMetaData.getColumnName(i)) + "\"");
				if (i != columnCount) {
					records.append(",");
				}
			}
			records.append("},");
		}

		records.setLength(records.length() - 1);
		records.append("]}");

		String htmlResponse = records.toString();

		httpExchange.getResponseHeaders().set("Content-Type", "application/json");
		httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
		httpExchange.sendResponseHeaders(200, htmlResponse.length());
		outputStream.write(htmlResponse.getBytes());
		outputStream.flush();
		outputStream.close();

		return "";
	}

	// parse URI query string and return a SELECT query string
	private String buildSelectQuery(String htmlQuery) {
		Map<String, List<String>> params = new LinkedHashMap<>();
		String[] pairs = htmlQuery.split("&");

		// Support multiple values for a key
		for (String pair : pairs) {
			int split = pair.indexOf('=');
			String key = pair.substring(0, split);
			String value = pair.substring(split + 1);

			if (!params.containsKey(key)) {
				params.put(key, new ArrayList<String>());
			}
			params.get(key).add(value);
		}

		// Build the select statement
		StringBuilder query = new StringBuilder(String.format("SELECT * FROM %s", params.get("table").get(0)));

		if (params.size() > 1) {
			query.append(" WHERE ");
			params.forEach((key, value) -> {
				if (!key.equals("table")) {
					for (int i = 0; i < value.size(); ++i) {
						if (i > 0) {
							query.append(" OR ");
						}
						query.append(key + "='" + value.get(i) + "'");
					}
				}
			});
		}
		return query.toString();
	}

	// Accept json to insert into database
	private String handlePostRequest(HttpExchange httpExchange) {
		return "";
	}
}
