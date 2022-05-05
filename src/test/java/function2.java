
import dao.impl.*;
import dao.services.*;
import pojo.Employees;
import pojo.LocatedIn;
import pojo.Shippers;
import pojo.Territories;
import pojo.Works;
import util.Dataset;

import org.junit.jupiter.api.Test;


import conditions.Operator;
import conditions.SimpleCondition;
import conditions.EmployeesAttribute;


public class function2{
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(function2.class);
	WorksService serviceW = new WorksServiceImpl();
	LocatedInService serviceL = new LocatedInServiceImpl();
	EmployeesService serviceEmpl = new EmployeesServiceImpl();
	Dataset<LocatedIn> dataset = new Dataset<LocatedIn>();
	Dataset<Employees> datasetEmpl = new Dataset<Employees>();
	SimpleCondition<EmployeesAttribute> cond = new SimpleCondition<EmployeesAttribute>(EmployeesAttribute.lastName, Operator.EQUALS , "Peacock");
	
	@Test
	public void testGetWhereLastName() {
		var territories = serviceW.getWorksListByEmployedCondition(cond);
		datasetEmpl = serviceEmpl.getEmployedListInWorksByEmployedCondition(cond);
		for (Works works : territories) {
			Territories t = works.getTerritories();
			dataset.add(serviceL.getLocatedInByTerritories(t));
		}
		for(LocatedIn loc : dataset){
			System.out.println(loc);
		}
		for(Employees emp: datasetEmpl) {
			System.out.println(emp);
		}
	}
}