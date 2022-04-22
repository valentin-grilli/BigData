import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Product;


public class ProductTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductTests.class);
	ProductService productService = new ProductServiceImpl();
	util.Dataset<Product> ProductDataset;
	
	@Test
	public void testGetAllProductInfo() {
		ProductDataset = productService.getProductList();
		ProductDataset.show();
	}
}