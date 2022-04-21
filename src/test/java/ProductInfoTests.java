import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.ProductInfo;
import pojo.Shipper;


public class ProductInfoTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductInfoTests.class);
	ProductInfoService productInfoService = new ProductInfoServiceImpl();
	util.Dataset<ProductInfo> ProductDataset;
	
	@Test
	public void testGetAllProductInfo() {
		ProductDataset = productInfoService.getProductInfoList();
		ProductDataset.show();
	}
}