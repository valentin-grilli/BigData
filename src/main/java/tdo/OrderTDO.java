package tdo;

import pojo.Order;
import java.util.List;
import java.util.ArrayList;

public class OrderTDO extends Order {
	private ArrayList<String> mongoSchema_Orders_customerRef_CustomerID = new ArrayList<>(); 
	public ArrayList<String>  getMongoSchema_Orders_customerRef_CustomerID() {
		return this.mongoSchema_Orders_customerRef_CustomerID;
	}

	public void setMongoSchema_Orders_customerRef_CustomerID( ArrayList<String>  mongoSchema_Orders_customerRef_CustomerID) {
		this.mongoSchema_Orders_customerRef_CustomerID = mongoSchema_Orders_customerRef_CustomerID;
	}

	private  String mongoSchema_Orders_shipperRef_ShipVia; 
	public  String getMongoSchema_Orders_shipperRef_ShipVia() {
		return this.mongoSchema_Orders_shipperRef_ShipVia;
	}

	public void setMongoSchema_Orders_shipperRef_ShipVia(  String mongoSchema_Orders_shipperRef_ShipVia) {
		this.mongoSchema_Orders_shipperRef_ShipVia = mongoSchema_Orders_shipperRef_ShipVia;
	}

	private  String mongoSchema_Orders_employeeRef_EmployeeRef; 
	public  String getMongoSchema_Orders_employeeRef_EmployeeRef() {
		return this.mongoSchema_Orders_employeeRef_EmployeeRef;
	}

	public void setMongoSchema_Orders_employeeRef_EmployeeRef(  String mongoSchema_Orders_employeeRef_EmployeeRef) {
		this.mongoSchema_Orders_employeeRef_EmployeeRef = mongoSchema_Orders_employeeRef_EmployeeRef;
	}

	private  String relSchema_Order_Details_orderR_OrderID; 
	public  String getRelSchema_Order_Details_orderR_OrderID() {
		return this.relSchema_Order_Details_orderR_OrderID;
	}

	public void setRelSchema_Order_Details_orderR_OrderID(  String relSchema_Order_Details_orderR_OrderID) {
		this.relSchema_Order_Details_orderR_OrderID = relSchema_Order_Details_orderR_OrderID;
	}

}
