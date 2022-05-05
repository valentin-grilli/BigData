import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.kenai.jffi.Array;

import conditions.EmployeesAttribute;
import conditions.Operator;
import conditions.SimpleCondition;
import dao.impl.BuyServiceImpl;

import dao.impl.RegisterServiceImpl;

import dao.services.BuyService;

import dao.services.RegisterService;
import pojo.Customers;
import pojo.Orders;
import pojo.Register;
import util.Dataset;

public class function4{
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(function4.class);
	SimpleCondition<EmployeesAttribute> empCond = new SimpleCondition<EmployeesAttribute>(EmployeesAttribute.firstName, Operator.EQUALS, "Margaret");
	RegisterService serviceRegister = new RegisterServiceImpl();
	BuyService serviceBuy = new BuyServiceImpl();
	Dataset<Customers> dataset = new Dataset<Customers>();
	ArrayList<Customers> list = new ArrayList<Customers>();
	Set<String> setString = new HashSet<String>();
	@Test
	public void testGetWhere() {
		var datasetRegister = serviceRegister.getRegisterList(null, empCond);
		for(Register register : datasetRegister) {
			Orders order = register.getProcessedOrder();
			Customers customer = serviceBuy.getBuyByBoughtOrder(order).getCustomer();
			if(setString.add(customer.getCustomerID())) {
				dataset.add(customer);
			}
		}
		for( Customers customer : dataset) {
			System.out.println(customer);
		}
	}
}