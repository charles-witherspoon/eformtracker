package com.example.eformtracker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.sun.net.httpserver.HttpServer;

public class Server {
	public static void main(String[] args) {
		
		// Create HTTP server that listens on port 4444
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 4444), 0);
			Database db = Database.getDatabase();
			
			// Create thread pool for asynchronous requests
			ThreadPoolExecutor threadPoolExecutor = 
					(ThreadPoolExecutor)Executors.newFixedThreadPool(10);
			
			server.createContext("/", new RequestHandler(db));
			server.setExecutor(threadPoolExecutor);
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
