package com.tomtom.athena.sample;

import static java.lang.ClassLoader.getSystemResourceAsStream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.lang3.text.StrSubstitutor;

import com.google.common.base.Stopwatch;

/**
 * Very basic app with pure jdbc connection that allows to connect to AWS Athena
 *
 */
public class SimpleAthenaQuery {

    private static Logger LOG = Logger.getLogger(SimpleAthenaQuery.class.toString());

    public static void main(
        final String... args) throws SQLException, IOException {
        final Properties connectionProperties = new Properties();
        connectionProperties.load(getSystemResourceAsStream("AthenaJDBC.properties"));

        final String awsRegion = "eu-west-1";

        final String jdbcUrl = "jdbc:awsathena://AwsRegion=" + awsRegion;

        LOG.info("Connecting  to AWS Athena: " + jdbcUrl + ".");
        LOG.info("Using connection properties: " + connectionProperties.toString());

        final Connection connection = DriverManager.getConnection(jdbcUrl, connectionProperties);

        final String query = "SELECT * from \"${schema}\".\"business\" limit 10";
        final ResultSet queryResults = executeQuery(connection, query);
        printQueryResults(queryResults);

        queryResults.close();
        connection.close();
    }

    private static void printQueryResults(
        final ResultSet queryResults) throws SQLException {
        final int columnCount = queryResults.getMetaData()
            .getColumnCount();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        while (queryResults.next()) {
            final StringBuilder logEntry = new StringBuilder("Row: ");
            for (int i = 1 ; i <= columnCount ; i++) {
                logEntry.append(queryResults.getObject(i));
                logEntry.append(", ");
            }
            LOG.info(logEntry.substring(0, logEntry.length() - 2));
        }
        stopwatch.stop();
        LOG.info(String.format("Results fetched in: %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    private static ResultSet executeQuery(
        final Connection connection,
        final String query1) throws SQLException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Statement statement = connection.createStatement();
        final ResultSet queryResults = statement.executeQuery(adjustQuerySchema(query1));
        stopwatch.stop();
        LOG.info(String.format("Query took: %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
        return queryResults;
    }

    private static String adjustQuerySchema(
        final String query) {
        final HashMap<String, String> substitutionMap = new HashMap<>();
        substitutionMap.put("schema", "yelp-parquet");
        final StrSubstitutor substitutor = new StrSubstitutor(substitutionMap);
        return substitutor.replace(query);
    }

}
