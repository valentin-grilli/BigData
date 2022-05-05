package tdo;

import pojo.Employees;
import java.util.List;
import java.util.ArrayList;

public class EmployeesTDO extends Employees {
	private  String mongoDB_Employees_manager_ReportsTo; 
	public  String getMongoDB_Employees_manager_ReportsTo() {
		return this.mongoDB_Employees_manager_ReportsTo;
	}

	public void setMongoDB_Employees_manager_ReportsTo(  String mongoDB_Employees_manager_ReportsTo) {
		this.mongoDB_Employees_manager_ReportsTo = mongoDB_Employees_manager_ReportsTo;
	}

	private  String mongoDB_Employees_manager_EmployeeID;
	public  String getMongoDB_Employees_manager_EmployeeID() {
		return this.mongoDB_Employees_manager_EmployeeID;
	}

	public void setMongoDB_Employees_manager_EmployeeID(  String mongoDB_Employees_manager_EmployeeID) {
		this.mongoDB_Employees_manager_EmployeeID = mongoDB_Employees_manager_EmployeeID;
	}

	private  String mongoDB_Orders_encoded_EmployeeID;
	public  String getMongoDB_Orders_encoded_EmployeeID() {
		return this.mongoDB_Orders_encoded_EmployeeID;
	}

	public void setMongoDB_Orders_encoded_EmployeeID(  String mongoDB_Orders_encoded_EmployeeID) {
		this.mongoDB_Orders_encoded_EmployeeID = mongoDB_Orders_encoded_EmployeeID;
	}

}
