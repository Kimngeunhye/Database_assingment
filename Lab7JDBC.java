/**************************************************
 * Copyright (c) 2023 KNU DACS Lab. To Present
 * All rights reserved. 
 **************************************************/

import javax.xml.transform.Result;
import java.sql.*; // import JDBC package

/**
 * Sample Code for JDBC Practice
 * @author yksuh
 */
public class Lab7JDBC {
    public static final String URL = "jdbc:oracle:thin:@localhost:1521:ord";
    public static final String USER_UNIVERSITY = "university";
    public static final String USER_PASSWD = "comp322";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;

        try {
            // Oracle JDBC 드라이버를 로드
            Class.forName("oracle.jdbc.OracleDriver");


            // 데이터베이스 연결 생성
            conn = DriverManager.getConnection(URL, USER_UNIVERSITY, USER_PASSWD);

            // 작업을 수행하기 위해 메서드를 호출
            doTask1(conn, stmt);
            System.out.println();
            doTask2(conn, stmt);

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                // Statement 객체를 닫습니다.
                if (stmt != null) {
                    stmt.close();
                }
                // Connection 객체를 닫습니다.
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void doTask1(Connection conn, Statement stmt) {
        try {
            stmt = conn.createStatement();

            // DEPARTMENT 테이블 생성
            String createDeptTable = "CREATE TABLE DEPARTMENT (" +
                    "DEPT_NAME VARCHAR2(255), " +
                    "DEPT_NUMBER NUMBER, " +
                    "MANAGER_SSN VARCHAR2(9), " +
                    "MANAGER_START_DATE DATE" +
                    ")";
            stmt.executeUpdate(createDeptTable);

            // EMPLOYEE 테이블 생성
            String createEmployeeTable = "CREATE TABLE EMPLOYEE (" +
                    "FNAME VARCHAR2(255), " +
                    "MINIT CHAR(1), " +
                    "LNAME VARCHAR2(255), " +
                    "SSN VARCHAR2(9), " +
                    "BDATE DATE, " +
                    "ADDRESS VARCHAR2(255), " +
                    "SEX CHAR(1), " +
                    "SALARY NUMBER, " +
                    "SUPERSSN VARCHAR2(9), " +
                    "DNO NUMBER" +
                    ")";
            stmt.executeUpdate(createEmployeeTable);

            // PROJECT 테이블 생성
            String createProjectTable = "CREATE TABLE PROJECT (" +
                    "PNAME VARCHAR2(255), " +
                    "PNUMBER NUMBER, " +
                    "PLOCATION VARCHAR2(255), " +
                    "DNUM NUMBER" +
                    ")";
            stmt.executeUpdate(createProjectTable);

            // DEPT_LOCATIONS 테이블 생성
            String createDeptLocationsTable = "CREATE TABLE DEPT_LOCATIONS (" +
                    "DNUMBER NUMBER, " +
                    "DLOCATION VARCHAR2(255)" +
                    ")";
            stmt.executeUpdate(createDeptLocationsTable);

            // WORKS_ON 테이블 생성
            String createWorksOnTable = "CREATE TABLE WORKS_ON (" +
                    "ESSN VARCHAR2(9), " +
                    "PNO NUMBER, " +
                    "HOURS NUMBER" +
                    ")";
            stmt.executeUpdate(createWorksOnTable);

            // DEPENDENT 테이블 생성
            String createDependentTable = "CREATE TABLE DEPENDENT (" +
                    "ESSN VARCHAR2(9), " +
                    "DEPENDENT_NAME VARCHAR2(255), " +
                    "SEX CHAR(1), " +
                    "BDATE DATE, " +
                    "RELATIONSHIP VARCHAR2(255)" +
                    ")";
            stmt.executeUpdate(createDependentTable);


            String sql = "INSERT INTO DEPARTMENT VALUES ('Headquarters', 1, '888665555', '1981-06-19')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPARTMENT VALUES ('Administration', 4, '987654321', '1995-01-01')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPARTMENT VALUES ('Research', 5, '333445555', '1988-05-22')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('James', 'E', 'Borg', '888665555', '1937-11-10', '450 Stone, Houston, TX', 'M', 55000, NULL, 1)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('Jennifer', 'S', 'Wallace', '987654321', '1941-06-20', '290 Berry, Bellaire, TX', 'F', 43000, 888665555, 4)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('Franklin', 'T', 'Wong', '333445555', '1968-01-19', '975 Fire Oak, Humble, TX', 'M', 40000, 888665555, 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('John', 'B', 'Smith', '123456789', '1965-01-09', '731 Fondren, Houston, TX', 'M', 30000, 333445555, 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('Alicia', 'J', 'Zelaya', '999887777', '1968-01-19', '3321 Castle, Spring, TX', 'F', 25000, 987654321, 4)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('Ramesh', 'K', 'Narayan', '666884444', '1962-09-15', '975 Fire Oak, Humble, TX', 'M', 38000, 333445555, 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('Joyce', 'A', 'English', '453453453', '1972-07-31', '5631 Rice, Houston, TX', 'F', 25000, 333445555, 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE VALUES ('Ahmad', 'V', 'Jabbar', '987987987', '1969-03-29', '980 Dallas, Houston, TX', 'M', 25000, 987654321, 4)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO PROJECT VALUES ('ProductX', 1, 'Bellaire', 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO PROJECT VALUES ('Computerization', 10, 'Stafford', 4)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO PROJECT VALUES ('Reorganization', 20, 'Houston', 1)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO PROJECT VALUES ('ProductY', 2, 'Sugarland', 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO PROJECT VALUES ('ProductZ', 3, 'Houston', 5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO PROJECT VALUES ('NewBenefits', 30, 'Stafford', 4)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPT_LOCATIONS VALUES (1, 'Houston')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPT_LOCATIONS VALUES (4, 'Stafford')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPT_LOCATIONS VALUES (5, 'Bellaire')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPT_LOCATIONS VALUES (5, 'Sugarland')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPT_LOCATIONS VALUES (5, 'Houston')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (123456789, 1, 32.5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (123456789, 2, 7.5)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (666884444, 3, 40.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (453453453, 1, 20.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (453453453, 2, 20.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (333445555, 2, 10.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (333445555, 3, 10.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (333445555, 10, 10.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (333445555, 20, 10.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (999887777, 30, 30.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (999887777, 10, 10.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (987987987, 10, 35.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (987987987, 30, 5.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (987654321, 30, 20.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (987654321, 20, 15.0)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO WORKS_ON VALUES (888665555, 20, NULL)";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (333445555, 'Alice', 'F', '1986-04-05', 'Daughter')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (333445555, 'Theodore', 'M', '1983-10-25', 'Son')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (333445555, 'Joy', 'F', '1958-05-03', 'Spouse')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (987654321, 'Abner', 'M', '1942-02-28', 'Spouse')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (123456789, 'Michael', 'M', '1988-01-04', 'Son')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (123456789, 'Alice', 'F', '1988-12-30', 'Daughter')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO DEPENDENT VALUES (123456789, 'Elizabeth', 'F', '1967-05-05', 'Spouse')";
            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void doTask2(Connection conn, Statement stmt) {
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT gender AS Sex, children AS Relationship, ROUND(AVG(salary), 2) AS AverageSalary " +
                    "FROM DEPENDENT " +  // Add space here
                    "GROUP BY gender, children " +  // Add spaces here
                    "ORDER BY AverageSalary DESC";
            rs = stmt.executeQuery(sql);
            System.out.println("<< query 1 result >>");
            while (rs.next()) {
                String sex = rs.getString("Sex");
                int relationship = rs.getInt("Children");
                double avgSalary = rs.getDouble("AverageSalary");
                System.out.println("Sex: " + sex + ", Children: " + relationship + ", Average Salary: " + avgSalary);
            }
            rs.close();

            System.out.println();

            // Q2: Complete your query.
            String sql2 = "SELECT E1.fname AS EmployeeFirstName, E1.lname AS EmployeeLastName, E1.address AS EmployeeAddress, E2.fname AS SupervisorFirstName, E2.lname AS SupervisorLastName " +
                    "FROM EMPLOYEE AS E1 " +
                    "INNER JOIN WORKS_ON AS W1 ON E1.ssn = W1.essn " +
                    "INNER JOIN PROJECT AS P ON W1.pno = P.pnumber " +
                    "INNER JOIN DEPARTMENT AS D ON P.dnum = D.dnumber " +
                    "LEFT JOIN EMPLOYEE AS E2 ON E1.superssn = E2.ssn " +
                    "WHERE D.dnumber = 1 " +
                    "GROUP BY E1.fname, E1.lname, E1.address, E2.fname, E2.lname " +
                    "HAVING COUNT(DISTINCT P.pnumber) = (SELECT COUNT(*) FROM PROJECT WHERE dnum = 1) " +
                    "ORDER BY E1.address ASC";
            rs = stmt.executeQuery(sql2);
            System.out.println("<< query 2 result >>");
            while (rs.next()) {
                String employeeFirstName = rs.getString("EmployeeFirstName");
                String employeeLastName = rs.getString("EmployeeLastName");
                String employeeAddress = rs.getString("EmployeeAddress");
                String supervisorFirstName = rs.getString("SupervisorFirstName");
                String supervisorLastName = rs.getString("SupervisorLastName");
                System.out.println("Employee: " + employeeFirstName + " " + employeeLastName + ", Address: " + employeeAddress);
                System.out.println("Supervisor: " + supervisorFirstName + " " + supervisorLastName);
            }
            rs.close();

            System.out.println();

            // Q3: Complete your query.
            sql = "SELECT D.dname AS DepartmentName, P.pname AS ProjectName, E.lname AS LastName, E.fname AS FirstName, E.salary AS Salary " +
                    "FROM DEPARTMENT AS D " +
                    "FULL OUTER JOIN PROJECT AS P ON D.dnumber = P.dnum " +
                    "FULL OUTER JOIN WORKS_ON AS W ON P.pnumber = W.pno " +
                    "FULL OUTER JOIN EMPLOYEE AS E ON W.essn = E.ssn " +
                    "WHERE D.plocation = 'Houston' " +
                    "ORDER BY DepartmentName ASC, Salary DESC";
            rs = stmt.executeQuery(sql);
            System.out.println("<< query 3 result >>");
            while (rs.next()) {
                String departmentName = rs.getString("DepartmentName");
                String projectName = rs.getString("ProjectName");
                String lastName = rs.getString("LastName");
                String firstName = rs.getString("FirstName");
                double salary = rs.getDouble("Salary");
                System.out.println("Department: " + departmentName);
                System.out.println("Project: " + projectName);
                System.out.println("Employee: " + firstName + " " + lastName);
                System.out.println("Salary: " + salary);
            }

            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
