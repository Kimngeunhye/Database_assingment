import java.sql.*;

public class MAIN {
    public static final String URL = "jdbc:oracle:thin:@localhost:1521:ord"; // Oracle Address
    public static final String USER_NAME = "university"; // Oracle user ID
    public static final String USER_PASSWD = "comp322"; // Oracle user password
    public static final String FINAL_EXAM_JDBC_TABLE_NAME = "HighAchieversCourses"; // The table to be created and used

    public static void main(String[] args) {
        Connection conn = null;

        /* Connect to oracle */
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("Success!");
        } catch (ClassNotFoundException e) {
            System.err.println("error = " + e.getMessage());
            System.exit(1);
        }

        try {
            conn = DriverManager.getConnection(URL, USER_NAME, USER_PASSWD);
            conn.setAutoCommit(false);
        } catch (SQLException ex) {
            System.err.println("Cannot get a connection : " + ex.getMessage());
            System.exit(1);
        }

        /* (a) */
        System.out.println("--- (a) ---");
        /* Drop table if existing */
        drop_table(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        Q1.create_timetable_tbl(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        print_table(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.println();

        try {
            conn.commit();
        } catch (SQLException ex) {
            System.err.println("Cannot get a connection : " + ex.getMessage());
            System.exit(1);
        }

        /* (b) */
        System.out.println("--- (b) ---");
        Q1.find_first_condition(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.print("-- Deleted Students --");
        System.out.println();
        print_table(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.println();

        /* (c) */
        System.out.println("--- (c) ---");
        Q1.find_second_condition(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.print("-- Deleted Students --");
        print_table(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.println();

        /* (d) */
        System.out.println("--- (d) ---");
        Q1.update_condition(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.print("\n-- Update Students --");
        print_table(conn, FINAL_EXAM_JDBC_TABLE_NAME);
        System.out.println();

    }

    /**
     * Print table contents
     *
     * @param conn                       connection object
     * @param FINAL_EXAM_JDBC_TABLE_NAME table name
     */
    public static void print_table(Connection conn, String FINAL_EXAM_JDBC_TABLE_NAME) {
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        int tuple_number = 0;
        System.out.println();
        System.out.println("TABLE " + FINAL_EXAM_JDBC_TABLE_NAME);
        try {
            pstmt = conn.prepareStatement("select * from " + FINAL_EXAM_JDBC_TABLE_NAME + " order by name");
            rs = pstmt.executeQuery();

            System.out.println(
                    "NAME\t\tDEPT_NAME\t\tTOT_CRED\tCOURSE_ID\tGRADE\tSEMESTER\tYEAR\t\tCREDITS\t\tPOSTREQ_ID");
            System.out.println(
                    "--------------------------------------------------------------------------------------------------------------------------------------------------------");
            while (rs.next()) {
                tuple_number = tuple_number + 1;

                String Name = rs.getString(1);
                String dept_name = rs.getString(2);
                int tot_cred = rs.getInt(3);
                String course_id = rs.getString(4);
                String grade = rs.getString(5);
                String semester = rs.getString(6);
                int year = rs.getInt(7);
                int cred = rs.getInt(8);
                String postreq_id = rs.getString(9);


                if (Name.length() < 8) {
                    if (dept_name.length() < 8) {
                        System.out.println(Name + "\t\t" +
                                dept_name + "\t\t\t" +
                                tot_cred + "\t\t" +
                                course_id + "\t\t" +
                                grade + "\t" +
                                semester + "\t\t" +
                                year + "\t\t" +
                                cred + "\t\t" +
                                postreq_id);
                    }
                    else if (dept_name.length() < 16) {
                        System.out.println(Name + "\t\t" +
                                dept_name + "\t\t" +
                                tot_cred + "\t\t" +
                                course_id + "\t\t" +
                                grade + "\t" +
                                semester + "\t\t" +
                                year + "\t\t" +
                                cred + "\t\t" +
                                postreq_id);
                    } else {
                        System.out.println(Name + "\t\t" +
                                dept_name + "\t" +
                                tot_cred + "\t\t" +
                                course_id + "\t\t" +
                                grade + "\t" +
                                semester + "\t\t" +
                                year + "\t\t" +
                                cred + "\t\t" +
                                postreq_id);
                    }

                }
                else {
                    if (dept_name.length() < 8) {
                        System.out.println(Name + "\t" +
                                dept_name + "\t\t\t" +
                                tot_cred + "\t\t" +
                                course_id + "\t\t" +
                                grade + "\t" +
                                semester + "\t\t" +
                                year + "\t\t" +
                                cred + "\t\t" +
                                postreq_id);
                    }
                    else if (dept_name.length() < 16) {
                        System.out.println(Name + "\t" +
                                dept_name + "\t\t" +
                                tot_cred + "\t\t" +
                                course_id + "\t\t" +
                                grade + "\t" +
                                semester + "\t\t" +
                                year + "\t\t" +
                                cred + "\t\t" +
                                postreq_id);
                    } else {
                        System.out.println(Name + "\t" +
                                dept_name + "\t" +
                                tot_cred + "\t\t" +
                                course_id + "\t\t" +
                                grade + "\t" +
                                semester + "\t\t" +
                                year + "\t\t" +
                                cred + "\t\t" +
                                postreq_id);
                    }

                }
            }
            if (rs != null)
                rs.close();
            if (pstmt != null)
                pstmt.close();
            System.out.println("# of Tuples : " + tuple_number);
        } catch (SQLException e) {
            System.err.println("(Print table)sql error = " + e.getMessage());
            System.exit(1);
        }
    }

    public static void drop_table(Connection conn, String FINAL_EXAM_JDBC_TABLE_NAME) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE " + FINAL_EXAM_JDBC_TABLE_NAME);
            System.out.println("DROP TABLE " + FINAL_EXAM_JDBC_TABLE_NAME + " COMPLETED");
            if (stmt != null)
                stmt.close();
        } catch (SQLException e) {
            System.err.println("(Drop table) sql error = " + e.getMessage());
        }
    }
}
