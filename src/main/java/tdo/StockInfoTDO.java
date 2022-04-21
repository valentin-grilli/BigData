package tdo;

import pojo.StockInfo;
import java.util.List;
import java.util.ArrayList;

public class StockInfoTDO extends StockInfo {
	private  String kv_stockInfoPairs_concerned_productid; 
	public  String getKv_stockInfoPairs_concerned_productid() {
		return this.kv_stockInfoPairs_concerned_productid;
	}

	public void setKv_stockInfoPairs_concerned_productid(  String kv_stockInfoPairs_concerned_productid) {
		this.kv_stockInfoPairs_concerned_productid = kv_stockInfoPairs_concerned_productid;
	}

}
