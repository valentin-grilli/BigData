package tdo;

import pojo.Orders;
import java.util.List;
import java.util.ArrayList;

public class OrdersTDO extends Orders {
	private  String mongoDB_Orders_encoded_EmployeeRef; 
	public  String getMongoDB_Orders_encoded_EmployeeRef() {
		return this.mongoDB_Orders_encoded_EmployeeRef;
	}

	public void setMongoDB_Orders_encoded_EmployeeRef(  String mongoDB_Orders_encoded_EmployeeRef) {
		this.mongoDB_Orders_encoded_EmployeeRef = mongoDB_Orders_encoded_EmployeeRef;
	}

	private  String mongoDB_Orders_deliver_ShipVia; 
	public  String getMongoDB_Orders_deliver_ShipVia() {
		return this.mongoDB_Orders_deliver_ShipVia;
	}

	public void setMongoDB_Orders_deliver_ShipVia(  String mongoDB_Orders_deliver_ShipVia) {
		this.mongoDB_Orders_deliver_ShipVia = mongoDB_Orders_deliver_ShipVia;
	}

	private  String relDB_Order_Details_order_OrderID; 
	public  String getRelDB_Order_Details_order_OrderID() {
		return this.relDB_Order_Details_order_OrderID;
	}

	public void setRelDB_Order_Details_order_OrderID(  String relDB_Order_Details_order_OrderID) {
		this.relDB_Order_Details_order_OrderID = relDB_Order_Details_order_OrderID;
	}

}
