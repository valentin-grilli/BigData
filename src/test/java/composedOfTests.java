import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Composed_of;


public class composedOfTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(composedOfTests.class);
	Composed_ofService Composed_ofService = new Composed_ofServiceImpl();
	util.Dataset<Composed_of> Composed_ofDataset;
	
	@Test
	public void testGetAllComposed_of() {
		Composed_ofDataset = Composed_ofService.getComposed_ofList();
		Composed_ofDataset.show();
	}
}