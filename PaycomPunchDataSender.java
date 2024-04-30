package com.dencofamily.paycom.brain.Punches;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONObject;

public class PaycomPunchDataSender implements Runnable {

    private static final String API_URL = "https://api.paycomonline.net/v4/rest/index.php/api/v1.1/punchimport";
    private static final String AUTHORIZATION_HEADER = "Basic ZTk4MjJmNzJiMjE2M2FhM2QxYmU1MzIxYTk0OTEzYTc4MmQxOGNkMjU0YzVjNTQ3Njg2M2UwMTVlOTkwMzY1ZTphY2E3NDA1ODg3ZGMyODM0YTU5MDJiN2I2NDc2MWZlYmNiZjNmMDBhMDE4ZWNmYmQyNDkxOTAwNTVmZWFlNmJh";
    private final String reportDate;
    

    public PaycomPunchDataSender(String reportDate) {
        this.reportDate = reportDate;
    }

    @Override
    public void run() {
    	 List<EmployeeData> employeeDataList = getEmployeeDataFromDatabase();
         
         String temp = null;

         for (int i = 0; i < employeeDataList.size(); i++) {
             // Get the current employee data
             EmployeeData employeeData = employeeDataList.get(i);
             
             // Generate JSON payload for the current employee
             String jsonPayload = generateJsonPayload(employeeData, employeeData.getPunchType());
             System.out.println("Sending JSON payload: " + jsonPayload);
             
             // Check if this is the first iteration or a different employee code is encountered
             if (i == 0 || !temp.equals(employeeData.getEecode())) {
                 
                 if (temp != null) {
                     updateStatusInDatabase(temp);
                 }
                 
                 // Update the stored employee code to the current one
                 temp = employeeData.getEecode();
             }
             
             // Send a POST request with the JSON payload
             try {
                 sendPostRequest(jsonPayload, employeeData);
             } catch (SQLException e) {
                 e.printStackTrace();
             }
             
             // If it's the last iteration, update the status for the last employee code
             if (i == employeeDataList.size() - 1) {
                 updateStatusInDatabase(temp);
             }
         }
         System.out.println("All Punches send Succesfully!!!");
     }

     // Helper method to update status in the database for a given employee code
     private void updateStatusInDatabase(String employeeCode) {
         String url = "jdbc:mysql://localhost:3306/ncr_db";
         String user = "root";
         String password = "root";
         
         // Prepare SQL update query
         String query = "UPDATE popeys_emp_punch_data SET status = 1 WHERE export_id = ?";
         
         try (Connection con = DriverManager.getConnection(url, user, password);
              PreparedStatement pst = con.prepareStatement(query)) {
             // Set the parameter in the prepared statement
             pst.setString(1, employeeCode);
             // Execute the update query
             pst.executeUpdate();
         } catch (SQLException e) {
             e.printStackTrace();
         }
     }


    private List<EmployeeData> getEmployeeDataFromDatabase() {
        List<EmployeeData> employeeDataList = new ArrayList<>();
        String url = "jdbc:mysql://localhost:3306/ncr_db";
        String user = "root";
        String password = "root";
        String query = "SELECT emp_name, store_name, emp_designation, export_id, in_time, out_time, break_in_time, break_out_time, report_date FROM popeys_emp_punch_data WHERE report_date = ? AND status != 1";
        try (Connection con = DriverManager.getConnection(url, user, password);
                PreparedStatement pst = con.prepareStatement(query)) {
               pst.setString(1, reportDate);
               try (ResultSet rs = pst.executeQuery()) {
                   while (rs.next()) {
                    String designation = rs.getString("emp_designation");
                    String deptcode;
                    switch (designation.toLowerCase()) {
                        case "foodhandlr":
                            deptcode = "0585";
                            break;
                        case "rm":
                            deptcode = "0551";
                            break;
                        case "manager":
                            deptcode = "0510";
                            break;
                        case "management":
                            deptcode = "0511";
                            break;
                        case "cook":
                            deptcode = "0585";
                            break;
                        default:
                            deptcode = "0585";
                            break;
                    }
                    String empName = rs.getString("emp_name");
                    String eecode = rs.getString("export_id");
                    String inTime = rs.getString("in_time");
                    String outTime = rs.getString("out_time");
                    String breakInTime = rs.getString("break_in_time");
                    String breakOutTime = rs.getString("break_out_time");
                    String report_date = rs.getString("report_date");
                    String store_name=rs.getString("store_name");

                    if (!inTime.equals("null")) {
                        employeeDataList.add(new EmployeeData(eecode, empName, store_name, "ID", getTimeUnixTimeStamp(inTime, report_date), deptcode, inTime, report_date));
                    }
                    if (!breakInTime.equals("null")) {
                        employeeDataList.add(new EmployeeData(eecode, empName, store_name, "OL", getTimeUnixTimeStamp(breakInTime, report_date), deptcode, breakInTime, report_date));
                    }
                    if (!breakOutTime.equals("null")) {
                        employeeDataList.add(new EmployeeData(eecode, empName, store_name, "IL", getTimeUnixTimeStamp(breakOutTime, report_date), deptcode, breakOutTime, report_date));
                    }
                    if (!outTime.equals("null")) {
                    	if(outTime.contains("AM")|| outTime.contains("am")) {
                    		report_date=getNextDay(report_date);
                    		employeeDataList.add(new EmployeeData(eecode, empName, store_name, "OD", getTimeUnixTimeStamp(outTime, report_date), deptcode, outTime, report_date));
                    	}else {
                    		employeeDataList.add(new EmployeeData(eecode, empName, store_name, "OD", getTimeUnixTimeStamp(outTime, report_date), deptcode, outTime, report_date));
                    	}
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (employeeDataList.isEmpty()) {
            System.out.println("No data found in the database for the specified report date.");
        }

        return employeeDataList;
    }

    private String getNextDay(String currentDate) throws ParseException {
        LocalDate date=LocalDate.parse(currentDate);
         LocalDate nextDay=date.plusDays(1);
         DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd");
         String formattedDate=nextDay.format(formatter);
         return formattedDate;
    }

    private void sendPostRequest(String jsonPayload,EmployeeData employeeData) throws SQLException {
    	EmployeeData empData=employeeData;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))
                .header("Content-Type", "application/json")
                .header("Authorization", AUTHORIZATION_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Response code: " + response.statusCode());
            System.out.println("Response body: " + response.body());

            JSONObject jsonResponse = new JSONObject(response.body());
 
            boolean result = jsonResponse.getBoolean("result");
            if (result) {
                JSONArray data = jsonResponse.getJSONArray("data");
                if (data.length() > 0) {
                    JSONObject punchData = data.getJSONObject(0);
                    long punchId = punchData.getLong("punchId");
                    String eecode=punchData.getString("eecode");
                    String punchtype = punchData.getString("punchtype");
                    String punchtime = punchData.getString("punchtime");
                    long punchTimeUnix=0l;;
                    try{
                    	punchTimeUnix=Long.parseLong(punchtime);
                    }catch (Exception e) {
					}
                    Date date = new Date(punchTimeUnix * 1000L);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("PST"));
                    String format = dateFormat.format(date);
                    
                    
                    saveToDatabase(punchId, eecode, punchtype, format, "Success");
                }
            } else {
                JSONArray errors = jsonResponse.getJSONArray("errors");
                if (errors.length() > 0) {
                    JSONObject error = errors.getJSONObject(0);
                    JSONObject punch = error.getJSONObject("punch");
                    String eecode = punch.getString("eecode");
                    String clocktype = punch.getString("clocktype");
                    String punchtype = punch.getString("punchtype");
                    String timezone = punch.getString("timezone");
                    String punchtime = punch.getString("punchtime");
                    JSONArray errorMessages = error.getJSONArray("errors");
                    if (errorMessages.length() > 0) {
                        String errorMessage = errorMessages.getString(0);
                        saveToDatabase("Error: " + errorMessage + " for EEcode: " + eecode, clocktype, punchtype, eecode, timezone, punchtime);
                        Emailer emailer=new Emailer();
                        
                        emailer.setEmployeeError(eecode,errorMessage);
                        emailer.setEmployeeErrorMessage(errorMessage, empData);
//                        emailer.emailSender(emailer);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
       
    }

    private void saveToDatabase(String message, String clocktype, String punchtype, String eecode, String timezone, String punchtime) throws SQLException {
    	 String url = "jdbc:mysql://localhost:3306/ncr_db";
         String user = "root";
         String password = "root";
         Connection con=DriverManager.getConnection(url, user, password);
        String query = "INSERT INTO popeys_emp_punch_data_error (message, clocktype, punchtype, eecode, timezone, punchtime) VALUES ( ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, message);
            pst.setString(2, clocktype);
            pst.setString(3, punchtype);
            pst.setString(4, eecode);
            pst.setString(5, timezone);
            pst.setString(6, punchtime);
            pst.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveToDatabase(long punchId, String eecode, String punchtype, String punchtime, String message) throws SQLException {
    	 String url = "jdbc:mysql://localhost:3306/ncr_db";
         String user = "root";
         String password = "root";
         Connection con=DriverManager.getConnection(url, user, password);
        String query = "INSERT INTO punch_data_success (punch_id, eecode, punchtype, punchtime, message) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pst = con.prepareStatement(query)) {
            pst.setLong(1, punchId);
            pst.setString(2, eecode);
            pst.setString(3, punchtype);
            pst.setString(4, punchtime);
            pst.setString(5, message);
            pst.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String generateJsonPayload(EmployeeData employeeData, String punchType) {
        return "[{\n" +
                "   \"clocktype\": \"K\",\n" +
                "   \"entrytype\": \"1\",\n" +
                "   \"punchtype\": \"" + punchType + "\",\n" +
//                "   \"eecode12\": \"" + employeeData.getEecode() + "\",\n" +
                "   \"eecode\": \"QWER\",\n"+
                "   \"timezone\": \"PST\",\n" +
                "   \"punchtime\": \"" + employeeData.getPunchTime() + "\",\n" +
                "   \"deptcode\": \"" + employeeData.getDeptCode() + "\",\n" +
                "   \"punchdesc\": \"API user\",\n" +
                "   \"taxprofid\": 0\n" +
                "}]";
    }

    static class EmployeeData {
        private String eecode;
        private String empName;
        private String store_name;
        private String punchType;
        private Long PunchTime;
        private String deptCode;
        private String Time;
        private String report_date;

        public EmployeeData(String eecode, String empName, String store_name, String punchType, Long PunchTime, String deptCode, String Time, String report_date) {
            this.eecode = eecode;
            this.empName = empName;
            this.punchType = punchType;
            this.PunchTime = PunchTime;
            this.deptCode = deptCode;
            this.store_name=store_name;
            this.Time=Time;
            this.report_date=report_date;
            
        }

        public String getEecode() {
            return eecode;
        }

        public String getEmpName() {
            return empName;
        }

        public String getPunchType() {
            return punchType;
        }

        public String getDeptCode() {
            return deptCode;
        }

        public Long getPunchTime() {
            return PunchTime;
        }
        public String getStoreName() {
            return store_name;
        }
        public String getTime() {
            return Time;
        }
        public String getReportDate() {
            return report_date;
        }
        
    }
    
    
    public static long getTimeUnixTimeStamp(String timeString, String reportDate) throws Exception {
         if (timeString == null || timeString.equals("null") || timeString.equals("0")) {
            return 0L;
        }

        String dateTimeString = reportDate + " " + timeString;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm a");
        dateFormat.setTimeZone(TimeZone.getTimeZone("PST"));
        Date parsedDateTime = dateFormat.parse(dateTimeString);

        long unixTimestampMillis = parsedDateTime.getTime();
        return unixTimestampMillis / 1000;
    }

}
