package tdo;

import pojo.Employee;
import java.util.List;
import java.util.ArrayList;

public class EmployeeTDO extends Employee {
	private  String mongoSchema_Employees_reportToRef_ReportsTo; 
	public  String getMongoSchema_Employees_reportToRef_ReportsTo() {
		return this.mongoSchema_Employees_reportToRef_ReportsTo;
	}

	public void setMongoSchema_Employees_reportToRef_ReportsTo(  String mongoSchema_Employees_reportToRef_ReportsTo) {
		this.mongoSchema_Employees_reportToRef_ReportsTo = mongoSchema_Employees_reportToRef_ReportsTo;
	}

	private  String mongoSchema_Employees_reportToRef_EmployeeID;
	public  String getMongoSchema_Employees_reportToRef_EmployeeID() {
		return this.mongoSchema_Employees_reportToRef_EmployeeID;
	}

	public void setMongoSchema_Employees_reportToRef_EmployeeID(  String mongoSchema_Employees_reportToRef_EmployeeID) {
		this.mongoSchema_Employees_reportToRef_EmployeeID = mongoSchema_Employees_reportToRef_EmployeeID;
	}

	private  String mongoSchema_Orders_employeeRef_EmployeeID;
	public  String getMongoSchema_Orders_employeeRef_EmployeeID() {
		return this.mongoSchema_Orders_employeeRef_EmployeeID;
	}

	public void setMongoSchema_Orders_employeeRef_EmployeeID(  String mongoSchema_Orders_employeeRef_EmployeeID) {
		this.mongoSchema_Orders_employeeRef_EmployeeID = mongoSchema_Orders_employeeRef_EmployeeID;
	}

}
