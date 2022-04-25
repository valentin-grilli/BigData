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
	
	
	// Left side 'EmployeeRef' of reference [employeeRef ]
	public Dataset<OrderTDO> getOrderTDOListOrderInEmployeeRefInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrderTDO> res = dataset.flatMap((FlatMapFunction<Row, OrderTDO>) r -> {
				Set<OrderTDO> list_res = new HashSet<OrderTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrderTDO order1 = new OrderTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							order1.setFreight(null);
						}else{
							order1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							order1.setOrderDate(null);
						}else{
							order1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							order1.setRequiredDate(null);
						}else{
							order1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							order1.setShipAddress(null);
						}else{
							order1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							order1.setId(null);
						}else{
							order1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							order1.setShipCity(null);
						}else{
							order1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							order1.setShipCountry(null);
						}else{
							order1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							order1.setShipName(null);
						}else{
							order1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							order1.setShipPostalCode(null);
						}else{
							order1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							order1.setShipRegion(null);
						}else{
							order1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							order1.setShippedDate(null);
						}else{
							order1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeRef for reference employeeRef . Reference field : EmployeeRef
					nestedRow =  r1;
					if(nestedRow != null) {
						order1.setMongoSchema_Orders_employeeRef_EmployeeRef(nestedRow.getAs("EmployeeRef") == null ? null : nestedRow.getAs("EmployeeRef").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(order1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrderTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	// Right side 'EmployeeID' of reference [employeeRef ]
	public Dataset<EmployeeTDO> getEmployeeTDOListEmployeeInEmployeeRefInOrdersFromMongoSchema(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeeServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeeTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeeTDO>) r -> {
				Set<EmployeeTDO> list_res = new HashSet<EmployeeTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeeTDO employee1 = new EmployeeTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employee.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employee1.setAddress(null);
						}else{
							employee1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employee1.setBirthDate(null);
						}else{
							employee1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employee1.setCity(null);
						}else{
							employee1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employee1.setCountry(null);
						}else{
							employee1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.id for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employee1.setId(null);
						}else{
							employee1.setId(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employee1.setExtension(null);
						}else{
							employee1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.firstname for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employee1.setFirstname(null);
						}else{
							employee1.setFirstname(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employee1.setHireDate(null);
						}else{
							employee1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employee1.setHomePhone(null);
						}else{
							employee1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.lastname for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employee1.setLastname(null);
						}else{
							employee1.setLastname(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employee1.setPhoto(null);
						}else{
							employee1.setPhoto(Util.getStringValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employee1.setPostalCode(null);
						}else{
							employee1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employee1.setRegion(null);
						}else{
							employee1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employee1.setSalary(null);
						}else{
							employee1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employee1.setTitle(null);
						}else{
							employee1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employee1.setNotes(null);
						}else{
							employee1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employee1.setPhotoPath(null);
						}else{
							employee1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employee1.setTitleOfCourtesy(null);
						}else{
							employee1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeID for reference employeeRef . Reference field : EmployeeID
					nestedRow =  r1;
					if(nestedRow != null) {
						employee1.setMongoSchema_Orders_employeeRef_EmployeeID(nestedRow.getAs("EmployeeID") == null ? null : nestedRow.getAs("EmployeeID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employee1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeeTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	
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
		
			order_refilter = new MutableBoolean(false);
			// For role 'order' in reference 'employeeRef'  B->A Scenario
			Dataset<OrderTDO> orderTDOemployeeReforder = handleService.getOrderTDOListOrderInEmployeeRefInOrdersFromMongoSchema(order_condition, order_refilter);
			Dataset<EmployeeTDO> employeeTDOemployeeRefemployee = handleService.getEmployeeTDOListEmployeeInEmployeeRefInOrdersFromMongoSchema(employee_condition, employee_refilter);
			
			Dataset<Row> res_employeeRef_temp = 
				employeeTDOemployeeRefemployee.join(orderTDOemployeeReforder
					.withColumnRenamed("id", "Order_id")
					.withColumnRenamed("freight", "Order_freight")
					.withColumnRenamed("orderDate", "Order_orderDate")
					.withColumnRenamed("requiredDate", "Order_requiredDate")
					.withColumnRenamed("shipAddress", "Order_shipAddress")
					.withColumnRenamed("shipCity", "Order_shipCity")
					.withColumnRenamed("shipCountry", "Order_shipCountry")
					.withColumnRenamed("shipName", "Order_shipName")
					.withColumnRenamed("shipPostalCode", "Order_shipPostalCode")
					.withColumnRenamed("shipRegion", "Order_shipRegion")
					.withColumnRenamed("shippedDate", "Order_shippedDate")
					.withColumnRenamed("logEvents", "Order_logEvents"),
					employeeTDOemployeeRefemployee.col("mongoSchema_Orders_employeeRef_EmployeeID").equalTo(orderTDOemployeeReforder.col("mongoSchema_Orders_employeeRef_EmployeeRef")));
		
			Dataset<Handle> res_employeeRef = res_employeeRef_temp.map(
				(MapFunction<Row, Handle>) r -> {
					Handle res = new Handle();
					Employee A = new Employee();
					Order B = new Order();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setAddress(Util.getStringValue(r.getAs("address")));
					A.setBirthDate(Util.getLocalDateValue(r.getAs("birthDate")));
					A.setCity(Util.getStringValue(r.getAs("city")));
					A.setCountry(Util.getStringValue(r.getAs("country")));
					A.setExtension(Util.getStringValue(r.getAs("extension")));
					A.setFirstname(Util.getStringValue(r.getAs("firstname")));
					A.setHireDate(Util.getLocalDateValue(r.getAs("hireDate")));
					A.setHomePhone(Util.getStringValue(r.getAs("homePhone")));
					A.setLastname(Util.getStringValue(r.getAs("lastname")));
					A.setPhoto(Util.getStringValue(r.getAs("photo")));
					A.setPostalCode(Util.getStringValue(r.getAs("postalCode")));
					A.setRegion(Util.getStringValue(r.getAs("region")));
					A.setSalary(Util.getDoubleValue(r.getAs("salary")));
					A.setTitle(Util.getStringValue(r.getAs("title")));
					A.setNotes(Util.getStringValue(r.getAs("notes")));
					A.setPhotoPath(Util.getStringValue(r.getAs("photoPath")));
					A.setTitleOfCourtesy(Util.getStringValue(r.getAs("titleOfCourtesy")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("Order_id")));
					B.setFreight(Util.getDoubleValue(r.getAs("Order_freight")));
					B.setOrderDate(Util.getLocalDateValue(r.getAs("Order_orderDate")));
					B.setRequiredDate(Util.getLocalDateValue(r.getAs("Order_requiredDate")));
					B.setShipAddress(Util.getStringValue(r.getAs("Order_shipAddress")));
					B.setShipCity(Util.getStringValue(r.getAs("Order_shipCity")));
					B.setShipCountry(Util.getStringValue(r.getAs("Order_shipCountry")));
					B.setShipName(Util.getStringValue(r.getAs("Order_shipName")));
					B.setShipPostalCode(Util.getStringValue(r.getAs("Order_shipPostalCode")));
					B.setShipRegion(Util.getStringValue(r.getAs("Order_shipRegion")));
					B.setShippedDate(Util.getLocalDateValue(r.getAs("Order_shippedDate")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Order_logEvents")));
						
					res.setEmployee(A);
					res.setOrder(B);
					return res;
				},Encoders.bean(Handle.class)
			);
		
					
			datasetsPOJO.add(res_employeeRef);
			
			Dataset<Handle> res_handle_employee;
			Dataset<Employee> res_Employee;
			
			
			//Join datasets or return 
			Dataset<Handle> res = fullOuterJoinsHandle(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employee> lonelyEmployee = null;
			Dataset<Order> lonelyOrder = null;
			
		
		
			
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
