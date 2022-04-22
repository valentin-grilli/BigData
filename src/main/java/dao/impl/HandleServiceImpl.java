package dao.impl;

import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.commons.lang3.StringUtils;
import util.Dataset;
import conditions.Condition;
import java.util.HashSet;
import java.util.Set;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.HandleAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeeTDO;
import tdo.HandleTDO;
import conditions.EmployeeAttribute;
import dao.services.EmployeeService;
import tdo.OrderTDO;
import tdo.HandleTDO;
import conditions.OrderAttribute;
import dao.services.OrderService;
import java.util.List;
import java.util.ArrayList;
import util.ScalaUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Row;
import org.apache.spark.sql.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import static com.mongodb.client.model.Updates.addToSet;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class HandleServiceImpl extends dao.services.HandleService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HandleServiceImpl.class);
	
	
	
	public Dataset<Handle> getHandleList(
		Condition<EmployeeAttribute> employee_condition,
		Condition<OrderAttribute> order_condition){
			HandleServiceImpl handleService = this;
			EmployeeService employeeService = new EmployeeServiceImpl();  
			OrderService orderService = new OrderServiceImpl();
			MutableBoolean employee_refilter = new MutableBoolean(false);
			List<Dataset<Handle>> datasetsPOJO = new ArrayList<Dataset<Handle>>();
			boolean all_already_persisted = false;
			MutableBoolean order_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Handle> res_handle_employee;
			Dataset<Employee> res_Employee;
			
			
			//Join datasets or return 
			Dataset<Handle> res = fullOuterJoinsHandle(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employee> lonelyEmployee = null;
			Dataset<Order> lonelyOrder = null;
			
			List<Dataset<Employee>> lonelyemployeeList = new ArrayList<Dataset<Employee>>();
			lonelyemployeeList.add(employeeService.getEmployeeListInEmployeesFromMyMongoDB(employee_condition, new MutableBoolean(false)));
			lonelyEmployee = EmployeeService.fullOuterJoinsEmployee(lonelyemployeeList);
			if(lonelyEmployee != null) {
				res = fullLeftOuterJoinBetweenHandleAndEmployee(res, lonelyEmployee);
			}	
		
			List<Dataset<Order>> lonelyorderList = new ArrayList<Dataset<Order>>();
			lonelyorderList.add(orderService.getOrderListInOrdersFromMyMongoDB(order_condition, new MutableBoolean(false)));
			lonelyOrder = OrderService.fullOuterJoinsOrder(lonelyorderList);
			if(lonelyOrder != null) {
				res = fullLeftOuterJoinBetweenHandleAndOrder(res, lonelyOrder);
			}	
		
			
			if(employee_refilter.booleanValue() || order_refilter.booleanValue())
				res = res.filter((FilterFunction<Handle>) r -> (employee_condition == null || employee_condition.evaluate(r.getEmployee())) && (order_condition == null || order_condition.evaluate(r.getOrder())));
			
		
			return res;
		
		}
	
	public Dataset<Handle> getHandleListByEmployeeCondition(
		Condition<EmployeeAttribute> employee_condition
	){
		return getHandleList(employee_condition, null);
	}
	
	public Dataset<Handle> getHandleListByEmployee(Employee employee) {
		Condition<EmployeeAttribute> cond = null;
		cond = Condition.simple(EmployeeAttribute.id, Operator.EQUALS, employee.getId());
		Dataset<Handle> res = getHandleListByEmployeeCondition(cond);
	return res;
	}
	public Dataset<Handle> getHandleListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getHandleList(null, order_condition);
	}
	
	public Handle getHandleByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Handle> res = getHandleListByOrderCondition(cond);
		List<Handle> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	
	
	public void deleteHandleList(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteHandleListByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteHandleList(employee_condition, null);
	}
	
	public void deleteHandleListByEmployee(pojo.Employee employee) {
		// TODO using id for selecting
		return;
	}
	public void deleteHandleListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteHandleList(null, order_condition);
	}
	
	public void deleteHandleByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
