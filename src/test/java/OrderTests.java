import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Customer;
import pojo.Order;
import pojo.ProductInfo;
import pojo.Shipper;


public class OrderTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderTests.class);
	OrderService orderService = new OrderServiceImpl();
	util.Dataset<Order> orderDataset;
	
	@Test
	public void testGetAllOrder() {
		orderDataset = orderService.getOrderList();
		orderDataset.show();
	}
}