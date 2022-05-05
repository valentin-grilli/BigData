package tdo;

import pojo.Shippers;
import java.util.List;
import java.util.ArrayList;

public class ShippersTDO extends Shippers {
	private  String mongoDB_Orders_deliver_ShipperID;
	public  String getMongoDB_Orders_deliver_ShipperID() {
		return this.mongoDB_Orders_deliver_ShipperID;
	}

	public void setMongoDB_Orders_deliver_ShipperID(  String mongoDB_Orders_deliver_ShipperID) {
		this.mongoDB_Orders_deliver_ShipperID = mongoDB_Orders_deliver_ShipperID;
	}

}
