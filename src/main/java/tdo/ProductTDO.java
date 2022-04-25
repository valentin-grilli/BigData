package tdo;

import pojo.Product;
import java.util.List;
import java.util.ArrayList;

public class ProductTDO extends Product {
	private  String relSchema_ProductsInfo_supplierR_SupplierRef; 
	public  String getRelSchema_ProductsInfo_supplierR_SupplierRef() {
		return this.relSchema_ProductsInfo_supplierR_SupplierRef;
	}

	public void setRelSchema_ProductsInfo_supplierR_SupplierRef(  String relSchema_ProductsInfo_supplierR_SupplierRef) {
		this.relSchema_ProductsInfo_supplierR_SupplierRef = relSchema_ProductsInfo_supplierR_SupplierRef;
	}

	private  String relSchema_Order_Details_productR_ProductID; 
	public  String getRelSchema_Order_Details_productR_ProductID() {
		return this.relSchema_Order_Details_productR_ProductID;
	}

	public void setRelSchema_Order_Details_productR_ProductID(  String relSchema_Order_Details_productR_ProductID) {
		this.relSchema_Order_Details_productR_ProductID = relSchema_Order_Details_productR_ProductID;
	}

	private  String relSchema_ProductsInfo_categoryR_CategoryRef; 
	public  String getRelSchema_ProductsInfo_categoryR_CategoryRef() {
		return this.relSchema_ProductsInfo_categoryR_CategoryRef;
	}

	public void setRelSchema_ProductsInfo_categoryR_CategoryRef(  String relSchema_ProductsInfo_categoryR_CategoryRef) {
		this.relSchema_ProductsInfo_categoryR_CategoryRef = relSchema_ProductsInfo_categoryR_CategoryRef;
	}

}
