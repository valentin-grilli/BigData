package tdo;

import pojo.Categories;
import java.util.List;
import java.util.ArrayList;

public class CategoriesTDO extends Categories {
	private  String relDB_ProductsInfo_isCategory_catid;
	public  String getRelDB_ProductsInfo_isCategory_catid() {
		return this.relDB_ProductsInfo_isCategory_catid;
	}

	public void setRelDB_ProductsInfo_isCategory_catid(  String relDB_ProductsInfo_isCategory_catid) {
		this.relDB_ProductsInfo_isCategory_catid = relDB_ProductsInfo_isCategory_catid;
	}

}
