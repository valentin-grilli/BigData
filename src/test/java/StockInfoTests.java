import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.StockInfo;


public class StockInfoTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StockInfoTests.class);
	StockInfoService stockInfoService = new StockInfoServiceImpl();
	util.Dataset<StockInfo> stockInfoDataset;
	
	@Test
	public void testGetAllCustomer() {
		stockInfoDataset = stockInfoService.getStockInfoList();
		stockInfoDataset.show();
	}
	
}