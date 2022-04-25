import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Are_in;


public class Are_inTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Are_inTests.class);
	Are_inService Are_inService = new Are_inServiceImpl();
	util.Dataset<Are_in> Are_inDataset;
	
	@Test
	public void testGetAllcategory() {
		Are_inDataset = Are_inService.getAre_inList();
		Are_inDataset.show();
	}
}