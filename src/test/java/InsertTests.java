import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Insert;


public class InsertTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertTests.class);
	InsertService InsertService = new InsertServiceImpl();
	util.Dataset<Insert> InsertDataset;
	
	@Test
	public void testGetAllcategory() {
		InsertDataset = InsertService.getInsertList();
		InsertDataset.show();
	}
}