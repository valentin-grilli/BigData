import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Belongs_to;


public class BelongsToTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BelongsToTests.class);
	Belongs_toService Belongs_toService = new Belongs_toServiceImpl();
	util.Dataset<Belongs_to> Belongs_toDataset;
	
	@Test
	public void testGetAllBelongs_to() {
		Belongs_toDataset = Belongs_toService.getBelongs_toList();
		Belongs_toDataset.show();
	}
}