package com.napier.sem;

import org.apache.commons.cli.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

public class App
{
    /**
     * Connection to MySQL database.
     */
    private Connection con = null;


    public static void main(String[] args)
    {
        // Manage command line options
        var options = new Options()
                .addOption("h",true,"Database host (default db)")
                .addOption("p",true,"Database port (default 3306)");

        var parser = new DefaultParser();
        CommandLine cmdLine;

        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            new HelpFormatter().printHelp("apache args...", options);
            return;
        }

        // Retrieve command line arguments
        String host;
        int port;
        if(cmdLine.hasOption('h')){
            host = cmdLine.getOptionValue('h');
        }else{
            host = "localhost";
        }
        if(cmdLine.hasOption('p')){
            port = Integer.parseInt(cmdLine.getOptionValue('p'));
        }else{
            port = 3306;
        }

        // Create new Application
        App a = new App();

        // Connect
        System.out.println(host+":"+port);
        a.connect(host+":"+port, 10000);

        // Retrieve list of managers
        ArrayList<Employee> employees = a.getSalariesByRole("Manager");
        a.outputEmployees(employees, "ManagerSalaries.md");

        // Disconnect from database
        a.disconnect();
    }


    /**
     * Connect to the MySQL database.
     */
    public void connect(String location, int delay) {
        try {
            // Load Database driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load SQL driver");
            System.exit(-1);
        }

        int retries = 10;
        boolean shouldWait = false;
        for (int i = 0; i < retries; ++i) {
            System.out.println("Connecting to database...");
            try {
                if (shouldWait) {
                    // Wait a bit for db to start
                    Thread.sleep(delay);
                }

                // Connect to database
                con = DriverManager.getConnection("jdbc:mysql://" + location
                                + "/employees?allowPublicKeyRetrieval=true&useSSL=false",
                        "root", "example");
                System.out.println("Successfully connected");
                break;
            } catch (SQLException sqle) {
                System.out.println("Failed to connect to database attempt " + i);
                System.out.println(sqle.getMessage());

                // Let's wait before attempting to reconnect
                shouldWait = true;
            } catch (InterruptedException ie) {
                System.out.println("Thread interrupted? Should not happen.");
            }
        }
    }


    /**
     * Disconnect from the MySQL database.
     */
    public void disconnect()
    {
        if (con != null)
        {
            try
            {
                // Close connection
                con.close();
            }
            catch (Exception e)
            {
                System.out.println("Error closing connection to database");
            }
        }
    }


    public Employee getEmployee(int ID)
    {
        try
        {
            // Create an SQL statement
            Statement stmt = con.createStatement();
            // Create string for SQL statement
            String strSelect = String.format("select first_name,last_name,\n" +
                    "(select title from titles where titles.emp_no = employees.emp_no and titles.to_date ='9999-01-01') as title,\n" +
                    "(select salary from salaries where salaries.emp_no = employees.emp_no and salaries.to_date = '9999-01-01') as salary,\n" +
                    "(select departments.dept_name from departments where departments.dept_no = dept_emp.dept_no) as dept_name,\n" +
                    "(select concat(employees.first_name,' ',employees.last_name) from employees inner join dept_manager on employees.emp_no=dept_manager.emp_no where dept_manager.dept_no=dept_emp.dept_no and dept_manager.to_date='9999-01-01') as manager\n" +
                    "from employees left outer join dept_emp on  employees.emp_no=dept_emp.emp_no where employees.emp_no = %d and (dept_emp.to_date ='9999-01-01' or dept_emp.to_date is NULL)", ID);

            // Execute SQL statement
            ResultSet rset = stmt.executeQuery(strSelect);
            // Return new employee if valid.
            // Check one is returned
            if (rset.next())
            {
                Employee emp = new Employee();
                emp.emp_no = ID;
                emp.first_name = rset.getString("first_name");
                emp.last_name = rset.getString("last_name");
                emp.title = rset.getString("title");
                emp.salary = rset.getInt("salary");
                emp.dept_name = rset.getString("dept_name");
                emp.manager = rset.getString("manager");
                return emp;
            }
            else
                return null;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.out.println("Failed to get employee details");
            return null;
        }
    }


    public void displayEmployee(Employee emp)
    {
        if (emp != null)
        {
            System.out.println(
                    emp.emp_no + " "
                            + emp.first_name + " "
                            + emp.last_name + "\n"
                            + emp.title + "\n"
                            + "Salary:" + emp.salary + "\n"
                            + emp.dept_name + "\n"
                            + "Manager: " + emp.manager + "\n");
        }
    }

    /**
     * Gets all the current employees and salaries.
     * @return A list of all employees and salaries, or null if there is an error.
     */
    public ArrayList<Employee> getAllSalaries()
    {
        try
        {
            // Create an SQL statement
            Statement stmt = con.createStatement();
            // Create string for SQL statement
            String strSelect =
                    "SELECT employees.emp_no, employees.first_name, employees.last_name, salaries.salary "
                            + "FROM employees, salaries "
                            + "WHERE employees.emp_no = salaries.emp_no AND salaries.to_date = '9999-01-01' "
                            + "ORDER BY employees.emp_no ASC";
            // Execute SQL statement
            ResultSet rset = stmt.executeQuery(strSelect);
            // Extract employee information
            ArrayList<Employee> employees = new ArrayList<Employee>();
            while (rset.next())
            {
                Employee emp = new Employee();
                emp.emp_no = rset.getInt("employees.emp_no");
                emp.first_name = rset.getString("employees.first_name");
                emp.last_name = rset.getString("employees.last_name");
                emp.salary = rset.getInt("salaries.salary");
                employees.add(emp);
            }
            return employees;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.out.println("Failed to get salary details");
            return null;
        }
    }

    public ArrayList<Employee> getSalariesByRole(String role)
    {
        try
        {
            // Create an SQL statement
            Statement stmt = con.createStatement();
            // Create string for SQL statement
            String strSelect =
                    String.format("SELECT employees.emp_no, employees.first_name, employees.last_name,\n" +
                            "titles.title, salaries.salary, departments.dept_name, dept_manager.emp_no\n" +
                            "FROM employees, salaries, titles, departments, dept_emp, dept_manager\n" +
                            "WHERE employees.emp_no = salaries.emp_no\n" +
                            "  AND salaries.to_date = '9999-01-01'\n" +
                            "  AND titles.emp_no = employees.emp_no\n" +
                            "  AND titles.to_date = '9999-01-01'\n" +
                            "  AND dept_emp.emp_no = employees.emp_no\n" +
                            "  AND dept_emp.to_date = '9999-01-01'\n" +
                            "  AND departments.dept_no = dept_emp.dept_no\n" +
                            "  AND dept_manager.dept_no = dept_emp.dept_no\n" +
                            "  AND dept_manager.to_date = '9999-01-01'\n" +
                            "  AND titles.title = '%s'",role);
            // Execute SQL statement
            ResultSet rset = stmt.executeQuery(strSelect);
            // Extract employee information
            ArrayList<Employee> employees = new ArrayList<Employee>();
            while (rset.next())
            {
                Employee emp = new Employee();
                emp.emp_no = rset.getInt("employees.emp_no");
                emp.first_name = rset.getString("employees.first_name");
                emp.last_name = rset.getString("employees.last_name");
                emp.title = rset.getString("title");
                emp.salary = rset.getInt("salaries.salary");
                emp.dept_name = rset.getString("dept_name");
                employees.add(emp);
            }
            return employees;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.out.println("Failed to get '+role+' details");
            return null;
        }
    }

    /**
     * Prints a list of employees.
     * @param employees The list of employees to print.
     */
    public void printSalaries(ArrayList<Employee> employees)
    {
        // Check if employees is not null
        if(employees==null){
            System.out.println("No employees");
            return;
        }
        // Print header
        System.out.printf("%-10s %-15s %-20s %-8s%n", "Emp No", "First Name", "Last Name", "Salary");
        // Loop over all employees in the list
        for (Employee emp : employees)
        {
            if (emp == null)
                continue;
            String emp_string =
                    String.format("%-10s %-15s %-20s %-8s",
                            emp.emp_no, emp.first_name, emp.last_name, emp.salary);
            System.out.println(emp_string);
        }
    }


    public void addEmployee(Employee emp)
    {
        try
        {
            Statement stmt = con.createStatement();
            String strUpdate =
                    "INSERT INTO employees (emp_no, first_name, last_name, birth_date, gender, hire_date) " +
                            "VALUES (" + emp.emp_no + ", '" + emp.first_name + "', '" + emp.last_name + "', " +
                            "'9999-01-01', 'M', '9999-01-01')";
            stmt.execute(strUpdate);
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.out.println("Failed to add employee");
        }
    }

    /**
     * Outputs to Markdown
     *
     * @param employees
     */
    public void outputEmployees(ArrayList<Employee> employees, String filename) {
        // Check employees is not null
        if (employees == null) {
            System.out.println("No employees");
            return;
        }

        StringBuilder sb = new StringBuilder();
        // Print header
        sb.append("| Emp No | First Name | Last Name | Title | Salary | Department | Manager |\r\n");
        sb.append("| --- | --- | --- | --- | --- | --- | --- |\r\n");
        // Loop over all employees in the list
        for (Employee emp : employees) {
            if (emp == null) continue;
            sb.append("| " + emp.emp_no + " | " +
                    emp.first_name + " | " + emp.last_name + " | " +
                    emp.title + " | " + emp.salary + " | "
                    + emp.dept_name + " | " + emp.manager + " |\r\n");
        }
        try {
            new File("./reports/").mkdir();
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("./reports/" + filename)));
            writer.write(sb.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}