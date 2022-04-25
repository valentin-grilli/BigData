import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Make_by;


public class Make_byTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Make_byTests.class);
	Make_byService Make_byService = new Make_byServiceImpl();
	util.Dataset<Make_by> Make_byDataset;
	
	@Test
	public void testGetAllcategory() {
		Make_byDataset = Make_byService.getMake_byList();
		Make_byDataset.show();
	}
}