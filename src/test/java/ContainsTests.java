import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Contains;


public class ContainsTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ContainsTests.class);
	ContainsService ContainsService = new ContainsServiceImpl();
	util.Dataset<Contains> ContainsDataset;
	
	@Test
	public void testGetAllcategory() {
		ContainsDataset = ContainsService.getContainsList();
		ContainsDataset.show();
	}
}