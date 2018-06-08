import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.text.StrSubstitutor;

import com.google.common.base.Stopwatch;

/**
 * Very basic app with pure jdbc connection that allows to connect to AWS Athena
 *
 */
public class SimpleAthenaQuery {

    public static void main(
        String... args) throws SQLException {
        Properties connectionProperties = new Properties();
        connectionProperties.put("S3OutputLocation", "[athenaBucketName]");
        connectionProperties.put("UID", "[privateKey]");
        connectionProperties.put("PWD", "[secretKey]");

        Connection connection = DriverManager.getConnection("jdbc:awsathena://AwsRegion=[AWS_REGION]", connectionProperties);

        final String query = "SELECT * from \"${schema}\".\"business\" limit 1000";
        ResultSet queryResults = executeQuery(connection, query);
        printQueryResults(queryResults);

        System.exit(0);
    }

    private static void printQueryResults(
        final ResultSet queryResults) throws SQLException {
        int columnCount = queryResults.getMetaData()
            .getColumnCount();
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (queryResults.next()) {
            for (int i = 1 ; i <= columnCount ; i++) {
                System.out.print(queryResults.getObject(i));
            }
            System.out.println();
        }
        stopwatch.stop();
        System.out.println(String.format("Results fetched in: %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    private static ResultSet executeQuery(
        final Connection connection,
        final String query1) throws SQLException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Statement statement = connection.createStatement();
        ResultSet queryResults = statement.executeQuery(adjustQuerySchema(query1));
        stopwatch.stop();
        System.out.println(String.format("Query took: %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
        return queryResults;
    }

    private static String adjustQuerySchema(
        String query) {
        HashMap<String, String> substitutionMap = new HashMap<>();
        substitutionMap.put("schema", "yelp-parquet");
        final StrSubstitutor substitutor = new StrSubstitutor(substitutionMap);
        return substitutor.replace(query);
    }

}
