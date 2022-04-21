import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Category;


public class CategoryTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CategoryTests.class);
	CategoryService categoryService = new CategoryServiceImpl();
	util.Dataset<Category> categoryDataset;
	
	@Test
	public void testGetAllcategory() {
		categoryDataset = categoryService.getCategoryList();
		categoryDataset.show();
	}
}