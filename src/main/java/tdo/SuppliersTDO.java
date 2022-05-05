package tdo;

import pojo.Suppliers;
import java.util.List;
import java.util.ArrayList;

public class SuppliersTDO extends Suppliers {
	private  String relDB_ProductsInfo_supply_SupplierID;
	public  String getRelDB_ProductsInfo_supply_SupplierID() {
		return this.relDB_ProductsInfo_supply_SupplierID;
	}

	public void setRelDB_ProductsInfo_supply_SupplierID(  String relDB_ProductsInfo_supply_SupplierID) {
		this.relDB_ProductsInfo_supply_SupplierID = relDB_ProductsInfo_supply_SupplierID;
	}

}
