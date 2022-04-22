import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Supplier;


public class SupplierTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SupplierTests.class);
	SupplierService SupplierService = new SupplierServiceImpl();
	util.Dataset<Supplier> SupplierDataset;
	
	@Test
	public void testGetAllSupplier() {
		SupplierDataset = SupplierService.getSupplierList();
		SupplierDataset.show();
	}
}