import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Region;


public class RegionTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegionTests.class);
	RegionService RegionService = new RegionServiceImpl();
	util.Dataset<Region> RegionDataset;
	
	@Test
	public void testGetAllRegion() {
		RegionDataset = RegionService.getRegionList();
		RegionDataset.show();
	}
}