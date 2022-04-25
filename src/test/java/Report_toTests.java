import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Report_to;


public class Report_toTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Report_toTests.class);
	Report_toService Report_toService = new Report_toServiceImpl();
	util.Dataset<Report_to> Report_toDataset;
	
	@Test
	public void testGetAllcategory() {
		Report_toDataset = Report_toService.getReport_toList();
		Report_toDataset.show();
	}
}