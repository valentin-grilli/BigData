import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Shipper;


public class ShipperTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShipperTests.class);
	ShipperService shipperService = new ShipperServiceImpl();
	util.Dataset<Shipper> ShipperDataset;
	
	@Test
	public void testGetAllShipper() {
		ShipperDataset = shipperService.getShipperList();
		ShipperDataset.show();
	}
}