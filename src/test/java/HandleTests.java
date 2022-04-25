import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Handle;


public class HandleTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HandleTests.class);
	HandleService HandleService = new HandleServiceImpl();
	util.Dataset<Handle> HandleDataset;
	
	@Test
	public void testGetAllHandle() {
		HandleDataset = HandleService.getHandleList();
		HandleDataset.show();
	}
}