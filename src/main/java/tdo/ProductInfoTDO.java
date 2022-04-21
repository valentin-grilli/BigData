package tdo;

import pojo.ProductInfo;
import java.util.List;
import java.util.ArrayList;

public class ProductInfoTDO extends ProductInfo {
	private  String kv_stockInfoPairs_concerned_ProductID;
	public  String getKv_stockInfoPairs_concerned_ProductID() {
		return this.kv_stockInfoPairs_concerned_ProductID;
	}

	public void setKv_stockInfoPairs_concerned_ProductID(  String kv_stockInfoPairs_concerned_ProductID) {
		this.kv_stockInfoPairs_concerned_ProductID = kv_stockInfoPairs_concerned_ProductID;
	}

}
