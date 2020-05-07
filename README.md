# Eform Tracker

Eform Tracker that reads eforms from a CSV file and inserts the records into a MySQL database. This project contains two sample eform csv files and database setup and cleanup SQL scripts.

I've added a new package (com.example.eformtracker) that handles HTTP GET requests and responds with a json object. The original EformInserter class is still functional; however, running the Server class will parse a URI query string and attempt to retrieve the data from the database.

#### The program requires the following:

- Apache Commons CSV
- MySQL Connector/J
