package tdo;

import pojo.Products;
import java.util.List;
import java.util.ArrayList;

public class ProductsTDO extends Products {
	private  String relDB_ProductsInfo_supply_SupplierRef; 
	public  String getRelDB_ProductsInfo_supply_SupplierRef() {
		return this.relDB_ProductsInfo_supply_SupplierRef;
	}

	public void setRelDB_ProductsInfo_supply_SupplierRef(  String relDB_ProductsInfo_supply_SupplierRef) {
		this.relDB_ProductsInfo_supply_SupplierRef = relDB_ProductsInfo_supply_SupplierRef;
	}

	private  String relDB_ProductsInfo_isCategory_CategoryRef; 
	public  String getRelDB_ProductsInfo_isCategory_CategoryRef() {
		return this.relDB_ProductsInfo_isCategory_CategoryRef;
	}

	public void setRelDB_ProductsInfo_isCategory_CategoryRef(  String relDB_ProductsInfo_isCategory_CategoryRef) {
		this.relDB_ProductsInfo_isCategory_CategoryRef = relDB_ProductsInfo_isCategory_CategoryRef;
	}

	private  String relDB_Order_Details_purchasedProducts_ProductID; 
	public  String getRelDB_Order_Details_purchasedProducts_ProductID() {
		return this.relDB_Order_Details_purchasedProducts_ProductID;
	}

	public void setRelDB_Order_Details_purchasedProducts_ProductID(  String relDB_Order_Details_purchasedProducts_ProductID) {
		this.relDB_Order_Details_purchasedProducts_ProductID = relDB_Order_Details_purchasedProducts_ProductID;
	}

}
