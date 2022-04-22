import dao.impl.*;
import dao.services.*;
import org.junit.jupiter.api.Test;

import pojo.Employee;


public class EmployeeTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeeTests.class);
	EmployeeService employeeService = new EmployeeServiceImpl();
	util.Dataset<Employee> employeeDataset;
	
	@Test
	public void testGetAllcategory() {
		employeeDataset = employeeService.getEmployeeList();
		employeeDataset.show();
	}
}