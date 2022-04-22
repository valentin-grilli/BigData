import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Customer;


public class CustomerTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerTests.class);
	CustomerService customerService = new CustomerServiceImpl();
	util.Dataset<Customer> CustomerDataset;
	
	@Test
	public void testGetAllCustomer() {
		CustomerDataset = customerService.getCustomerList();
		CustomerDataset.show();
	}
}