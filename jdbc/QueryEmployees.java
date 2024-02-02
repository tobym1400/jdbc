import java.io.*;
import java.sql.*;
import java.util.*;

public class QueryEmployees {


  public static final String propsFile = "jdbc.properties";




  public static Connection getConnection() throws IOException, SQLException
  {
    // Load properties

    FileInputStream in = new FileInputStream(propsFile);
    Properties props = new Properties();
    props.load(in);

    // Define JDBC driver

    String drivers = props.getProperty("jdbc.drivers");
    if (drivers != null)
      System.setProperty("jdbc.drivers", drivers);
      // Setting standard system property jdbc.drivers
      // is an alternative to loading the driver manually
      // by calling Class.forName()

    // Obtain access parameters and use them to create connection

    String url = props.getProperty("jdbc.url");
    String user = props.getProperty("jdbc.user");
    String password = props.getProperty("jdbc.password");

    return DriverManager.getConnection(url, user, password);
  }

  public static void list(Connection database)
   throws SQLException
  {
    Statement statement = database.createStatement();
    ResultSet results = statement.executeQuery(
     "SELECT * FROM employees");
    while (results.next()) {
      String surname = results.getString("surname");
      String forename = results.getString("forename");
      int nbHours = results.getInt("numberHours");
      double baseRate = results.getDouble("baseRate");
      System.out.println(surname + " " + forename + " " + nbHours + " " + baseRate);
    }
    statement.close();
  }  

  public static void findNames(String surname, Connection database)
   throws SQLException
  {
    Statement statement = database.createStatement();
    ResultSet results = statement.executeQuery(
     "SELECT * FROM employees WHERE surname = '" + surname + "'");
    while (results.next()) {
      String forename = results.getString("forename");
      int nbHours = results.getInt("numberHours");
      double baseRate = results.getDouble("baseRate");
      System.out.println(surname + " " + forename + " " + nbHours + " " + baseRate);
    }
    statement.close();
  }

  public static void avgHours(Connection database)
   throws SQLException
  {
    Statement statement = database.createStatement();
    ResultSet results = statement.executeQuery(
     "SELECT AVG(numberHours) FROM employees");
    while (results.next()) {
      String avgHours = results.getString("AVG(numberHours)");
      System.out.println(avgHours);
    }
    statement.close();
  }

  public static void avgBaseRate(Connection database)
   throws SQLException
  {
    Statement statement = database.createStatement();
    ResultSet results = statement.executeQuery(
     "SELECT AVG(baseRate) FROM employees");
    while (results.next()) {
      String avgBaseRate = results.getString("AVG(baseRate)");
      System.out.println(avgBaseRate);
    }
    statement.close();
  }

  public static void totalWages(Connection database)
   throws SQLException
  {
    Statement statement = database.createStatement();
    ResultSet results = statement.executeQuery(
     "SELECT SUM(numberHours), AVG(baseRate) FROM employees");
    while (results.next()) {
      String sumHours = results.getString("SUM(numberHours)");
      String avgBaseRate = results.getString("AVG(baseRate)");
      int hoursTotal = Integer.parseInt(sumHours);
      double baseRateAvg = Double.valueOf(avgBaseRate).doubleValue();
      double totalWages = hoursTotal * baseRateAvg;
      System.out.println(totalWages);
    }
    statement.close();
  }

  /**
   * Main program.
   */

  public static void main(String[] argv)
  {
      /*if (argv.length == 0) {
      System.err.println("usage: java QueryDB <forename>");
      System.exit(1);
      }*/

    Connection connection = null;
 
    try {
      connection = getConnection();
      list(connection);
      findNames(argv[0], connection);
      avgHours(connection);
      avgBaseRate(connection);
      totalWages(connection);
    }
    catch (Exception error) {
      error.printStackTrace();
    }
    finally {

      // This will always execute, even if an exception has
      // been thrown elsewhere in the code - so this is
      // the ideal place to close the connection to the DB...

      if (connection != null) {
        try {
          connection.close();
        }
        catch (Exception error) {}
      }
    }
  }


}
