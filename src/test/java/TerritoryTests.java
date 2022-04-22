import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Territory;


public class TerritoryTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TerritoryTests.class);
	TerritoryService TerritoryService = new TerritoryServiceImpl();
	util.Dataset<Territory> TerritoryDataset;
	
	@Test
	public void testGetAllTerritory() {
		TerritoryDataset = TerritoryService.getTerritoryList();
		TerritoryDataset.show();
	}
}