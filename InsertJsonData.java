package com.dencofamily.paycom.brain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class InsertJsonData {
    public static void main(String[] args) throws SQLException {
        try {
            // API URL
            URL url = new URL("https://dc01.rmdatacentral.com/portal1246/api/Reports/eda01fcc-9f23-4f3e-a054-9b852788b0d0//Result?@FromDate=2024-03-28&@ThruDate=2024-03-28");

            // Establish HTTP connection
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("accept", "application/json");
            connection.setRequestProperty("DCUser", "apps@popeyes");
            connection.setRequestProperty("DCPassword", "de!NL!!vvc7&2x*j");
            connection.setRequestProperty("DCKey", "v3G2Z3bHvEwuKbV2eUdYKHRxhE-nuC0xdEtdCEjNI96pO5servKREL58Q3aY51Zi8o0");

            // Check response code
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            // Read response
            StringBuilder response = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    responseCode == HttpURLConnection.HTTP_OK ? connection.getInputStream() : connection.getErrorStream()))) {
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    response.append(inputLine);
                }
            }
            System.out.println(response.toString());

            // Parse JSON response
            JSONObject jsonObject = new JSONObject(response.toString());
            JSONArray rowsArray = jsonObject.getJSONArray("Data").getJSONObject(0).getJSONArray("Rows");

            // Database connection parameters
            String dbUrl = "jdbc:mysql://localhost:3306/ncrpunches";
            String dbUser = "root";
            String dbPassword = "root";

            // Establish database connection
            try (Connection dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                // Iterate over rows and insert into database
                for (int i = 0; i < rowsArray.length(); i++) {
                    JSONObject rowObject = rowsArray.getJSONObject(i);
                    String sql = "INSERT INTO employeeattendance (UnitID, UnitNumber, UnitName, BusinessDate, POSEmployeeCode, POSEmployeeName, POSJobCode, POSJobName, ClockIn, ClockOut, POSRegularHours, POSOTHours, POSTotalHours, POSRegularRate, POSRegularAmount, POSOTRate, POSOTAmount, POSTotalAmount, DCOverwritePunch, POSEmployeeFirstName, POSEmployeeLastName, POSSSN, DCJobID, POSPayrollID, POSBreaks, BreakExempt, ImportedBy, ImportedOn, Department, EmployeeType, RMDCJob, UnitCustom4, Valid, Completed, Age, POSGender, ReportOrigin) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement preparedStatement = dbConnection.prepareStatement(sql)) {
                        preparedStatement.setInt(1, rowObject.getInt("UnitID"));
                        preparedStatement.setString(2, rowObject.getString("UnitNumber"));
                        preparedStatement.setString(3, rowObject.getString("UnitName"));
                        preparedStatement.setString(4, rowObject.getString("BusinessDate"));
                        preparedStatement.setString(5, rowObject.getString("POSEmployeeCode"));
                        preparedStatement.setString(6, rowObject.getString("POSEmployeeName"));
                        preparedStatement.setString(7, rowObject.getString("POSJobCode"));
                        preparedStatement.setString(8, rowObject.getString("POSJobName"));
                        preparedStatement.setString(9, rowObject.getString("ClockIn"));
                        preparedStatement.setString(10, rowObject.getString("ClockOut"));
                        preparedStatement.setDouble(11, rowObject.getDouble("POSRegular Hours"));
                        preparedStatement.setInt(12, rowObject.getInt("POSOTHours"));
                        preparedStatement.setDouble(13, rowObject.getDouble("POSTotalHours"));
//                        preparedStatement.setDouble(14, rowObject.getDouble("POSRegularRate"));
                        if (rowObject.has("POSRegularRate")) {
                            preparedStatement.setDouble(14, rowObject.getDouble("POSRegularRate"));
                        } else {
                        double defaultValue = 0.0; // Set default value here
                        double posRegularRate = rowObject.has("POSRegularRate") ? rowObject.getDouble("POSRegularRate") : defaultValue;
                        preparedStatement.setDouble(14, posRegularRate);
                        }
                        preparedStatement.setDouble(15, rowObject.getDouble("POSRegular$"));
//                        preparedStatement.setDouble(16, rowObject.getDouble("POSOTRate"));
                        if (rowObject.has("POSOTRate")) {
                            preparedStatement.setDouble(16, rowObject.getDouble("POSRegularRate"));
                        } else {
                        double defaultValue = 0.0; // Set default value here
                        double posRegularRate = rowObject.has("POSOTRate") ? rowObject.getDouble("POSOTRate") : defaultValue;
                        preparedStatement.setDouble(16, posRegularRate);
                        }
                        preparedStatement.setDouble(17, rowObject.getDouble("POSOT$"));
                        preparedStatement.setDouble(18, rowObject.getDouble("POSTotal$"));
                        preparedStatement.setString(19, rowObject.getString("DCOverwritePunch"));
                        preparedStatement.setString(20, rowObject.getString("POSEmployeeFirstName"));
                        preparedStatement.setString(21, rowObject.getString("POSEmployeeLastName"));
                        preparedStatement.setString(22, rowObject.getString("POSSSN"));
                        preparedStatement.setString(23, rowObject.getString("DCJobID"));
                        preparedStatement.setString(24, rowObject.getString("POSPayrollID"));
                        preparedStatement.setInt(25, rowObject.getInt("POS Breaks"));
                        preparedStatement.setBoolean(26, rowObject.getBoolean("BreakExempt"));
                        preparedStatement.setString(27, rowObject.getString("ImportedBy"));
                        preparedStatement.setString(28, rowObject.getString("ImportedOn"));
//                        preparedStatement.setString(29, rowObject.getString("Department"));
                        
                        if (rowObject.has("Department")) {
                        	 preparedStatement.setString(29, rowObject.getString("Department"));
                        } else {
                         String defaultValue = "NULL"; // Set default value here
                         String Department = rowObject.has("Department") ? rowObject.getString("Department") : defaultValue;
                        preparedStatement.setString(29, Department);
                        }
                        
                        preparedStatement.setString(30, rowObject.getString("Employee Type"));
                        preparedStatement.setString(31, rowObject.getString("RMDC Job"));
                        preparedStatement.setString(32, rowObject.getString("Unit Custom4"));
                        preparedStatement.setString(33, rowObject.getString("Valid"));
                        preparedStatement.setString(34, rowObject.getString("Completed"));
                        preparedStatement.setInt(35, rowObject.getInt("Age"));
                        preparedStatement.setString(36, rowObject.getString("POSGender"));
                        preparedStatement.setString(37, rowObject.getString("Report Origin  "));

                        // Execute the prepared statement
                        preparedStatement.executeUpdate();
                        System.out.println("Record inserted successfully");
                    } catch (Exception e) {
                        System.out.println("Error inserting record: " + e.getMessage());
                        continue;
                    }
                }
            } catch (SQLException e) {
                System.out.println("Connection failed: " + e.getMessage());
            }
        } catch (IOException | JSONException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
