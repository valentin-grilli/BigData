import dao.impl.*;
import dao.services.*;
import pojo.Products;


import org.junit.jupiter.api.Test;


public class function3 {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(function3.class);
	ProductsService service = new ProductsServiceImpl();
	util.Dataset<Products> products;
	
	@Test
	public void testGetAll() {
		products = service.getProductsList();
		for(Products product : products) {
			System.out.println(product);
		}
	}
}