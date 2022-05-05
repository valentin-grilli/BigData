
import dao.impl.*;
import dao.services.*;
import pojo.Shippers;

import java.util.Iterator;

import org.junit.jupiter.api.Test;


public class fucntion1 {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(fucntion1.class);
	ShippersService service = new ShippersServiceImpl();
	util.Dataset<Shippers> shippers;
	
	@Test
	public void testGetAll() {
		shippers = service.getShippersList();
		for(Shippers shipper : shippers) {
			System.out.println(shipper);
		}
	}
}