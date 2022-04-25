package tdo;

import pojo.Shipper;
import java.util.List;
import java.util.ArrayList;

public class ShipperTDO extends Shipper {
	private  String mongoSchema_Orders_shipperRef_ShipperID;
	public  String getMongoSchema_Orders_shipperRef_ShipperID() {
		return this.mongoSchema_Orders_shipperRef_ShipperID;
	}

	public void setMongoSchema_Orders_shipperRef_ShipperID(  String mongoSchema_Orders_shipperRef_ShipperID) {
		this.mongoSchema_Orders_shipperRef_ShipperID = mongoSchema_Orders_shipperRef_ShipperID;
	}

}
