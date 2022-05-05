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
import conditions.RegisterAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.RegisterTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.EmployeesTDO;
import tdo.RegisterTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
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

public class RegisterServiceImpl extends dao.services.RegisterService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegisterServiceImpl.class);
	
	
	// Left side 'EmployeeRef' of reference [encoded ]
	public Dataset<OrdersTDO> getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrdersTDO> res = dataset.flatMap((FlatMapFunction<Row, OrdersTDO>) r -> {
				Set<OrdersTDO> list_res = new HashSet<OrdersTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrdersTDO orders1 = new OrdersTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							orders1.setId(null);
						}else{
							orders1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							orders1.setOrderDate(null);
						}else{
							orders1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							orders1.setRequiredDate(null);
						}else{
							orders1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							orders1.setShippedDate(null);
						}else{
							orders1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							orders1.setFreight(null);
						}else{
							orders1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							orders1.setShipName(null);
						}else{
							orders1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							orders1.setShipAddress(null);
						}else{
							orders1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							orders1.setShipCity(null);
						}else{
							orders1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							orders1.setShipRegion(null);
						}else{
							orders1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							orders1.setShipPostalCode(null);
						}else{
							orders1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							orders1.setShipCountry(null);
						}else{
							orders1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeRef for reference encoded . Reference field : EmployeeRef
					nestedRow =  r1;
					if(nestedRow != null) {
						orders1.setMongoDB_Orders_encoded_EmployeeRef(nestedRow.getAs("EmployeeRef") == null ? null : nestedRow.getAs("EmployeeRef").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(orders1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrdersTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	// Right side 'EmployeeID' of reference [encoded ]
	public Dataset<EmployeesTDO> getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeesTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeesTDO>) r -> {
				Set<EmployeesTDO> list_res = new HashSet<EmployeesTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeesTDO employees1 = new EmployeesTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employees.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employees1.setEmployeeID(null);
						}else{
							employees1.setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employees1.setLastName(null);
						}else{
							employees1.setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employees1.setFirstName(null);
						}else{
							employees1.setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employees1.setTitle(null);
						}else{
							employees1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employees1.setTitleOfCourtesy(null);
						}else{
							employees1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employees1.setBirthDate(null);
						}else{
							employees1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employees1.setHireDate(null);
						}else{
							employees1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employees1.setHomePhone(null);
						}else{
							employees1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employees1.setExtension(null);
						}else{
							employees1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employees1.setPhoto(null);
						}else{
							employees1.setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employees1.setNotes(null);
						}else{
							employees1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employees1.setPhotoPath(null);
						}else{
							employees1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employees1.setSalary(null);
						}else{
							employees1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employees1.setAddress(null);
						}else{
							employees1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employees1.setCity(null);
						}else{
							employees1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employees1.setRegion(null);
						}else{
							employees1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employees1.setPostalCode(null);
						}else{
							employees1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employees1.setCountry(null);
						}else{
							employees1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeID for reference encoded . Reference field : EmployeeID
					nestedRow =  r1;
					if(nestedRow != null) {
						employees1.setMongoDB_Orders_encoded_EmployeeID(nestedRow.getAs("EmployeeID") == null ? null : nestedRow.getAs("EmployeeID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employees1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeesTDO.class));
		res= res.dropDuplicates(new String[]{"employeeID"});
		return res;
	}
	
	
	
	
	public Dataset<Register> getRegisterList(
		Condition<OrdersAttribute> processedOrder_condition,
		Condition<EmployeesAttribute> employeeInCharge_condition){
			RegisterServiceImpl registerService = this;
			OrdersService ordersService = new OrdersServiceImpl();  
			EmployeesService employeesService = new EmployeesServiceImpl();
			MutableBoolean processedOrder_refilter = new MutableBoolean(false);
			List<Dataset<Register>> datasetsPOJO = new ArrayList<Dataset<Register>>();
			boolean all_already_persisted = false;
			MutableBoolean employeeInCharge_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'processedOrder' in reference 'encoded'. A->B Scenario
			employeeInCharge_refilter = new MutableBoolean(false);
			Dataset<OrdersTDO> ordersTDOencodedprocessedOrder = registerService.getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(processedOrder_condition, processedOrder_refilter);
			Dataset<EmployeesTDO> employeesTDOencodedemployeeInCharge = registerService.getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(employeeInCharge_condition, employeeInCharge_refilter);
			
			Dataset<Row> res_encoded_temp = ordersTDOencodedprocessedOrder.join(employeesTDOencodedemployeeInCharge
					.withColumnRenamed("employeeID", "Employees_employeeID")
					.withColumnRenamed("lastName", "Employees_lastName")
					.withColumnRenamed("firstName", "Employees_firstName")
					.withColumnRenamed("title", "Employees_title")
					.withColumnRenamed("titleOfCourtesy", "Employees_titleOfCourtesy")
					.withColumnRenamed("birthDate", "Employees_birthDate")
					.withColumnRenamed("hireDate", "Employees_hireDate")
					.withColumnRenamed("address", "Employees_address")
					.withColumnRenamed("city", "Employees_city")
					.withColumnRenamed("region", "Employees_region")
					.withColumnRenamed("postalCode", "Employees_postalCode")
					.withColumnRenamed("country", "Employees_country")
					.withColumnRenamed("homePhone", "Employees_homePhone")
					.withColumnRenamed("extension", "Employees_extension")
					.withColumnRenamed("photo", "Employees_photo")
					.withColumnRenamed("notes", "Employees_notes")
					.withColumnRenamed("photoPath", "Employees_photoPath")
					.withColumnRenamed("salary", "Employees_salary")
					.withColumnRenamed("logEvents", "Employees_logEvents"),
					ordersTDOencodedprocessedOrder.col("mongoDB_Orders_encoded_EmployeeRef").equalTo(employeesTDOencodedemployeeInCharge.col("mongoDB_Orders_encoded_EmployeeID")));
		
			Dataset<Register> res_encoded = res_encoded_temp.map(
				(MapFunction<Row, Register>) r -> {
					Register res = new Register();
					Orders A = new Orders();
					Employees B = new Employees();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setOrderDate(Util.getLocalDateValue(r.getAs("orderDate")));
					A.setRequiredDate(Util.getLocalDateValue(r.getAs("requiredDate")));
					A.setShippedDate(Util.getLocalDateValue(r.getAs("shippedDate")));
					A.setFreight(Util.getDoubleValue(r.getAs("freight")));
					A.setShipName(Util.getStringValue(r.getAs("shipName")));
					A.setShipAddress(Util.getStringValue(r.getAs("shipAddress")));
					A.setShipCity(Util.getStringValue(r.getAs("shipCity")));
					A.setShipRegion(Util.getStringValue(r.getAs("shipRegion")));
					A.setShipPostalCode(Util.getStringValue(r.getAs("shipPostalCode")));
					A.setShipCountry(Util.getStringValue(r.getAs("shipCountry")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setEmployeeID(Util.getIntegerValue(r.getAs("Employees_employeeID")));
					B.setLastName(Util.getStringValue(r.getAs("Employees_lastName")));
					B.setFirstName(Util.getStringValue(r.getAs("Employees_firstName")));
					B.setTitle(Util.getStringValue(r.getAs("Employees_title")));
					B.setTitleOfCourtesy(Util.getStringValue(r.getAs("Employees_titleOfCourtesy")));
					B.setBirthDate(Util.getLocalDateValue(r.getAs("Employees_birthDate")));
					B.setHireDate(Util.getLocalDateValue(r.getAs("Employees_hireDate")));
					B.setAddress(Util.getStringValue(r.getAs("Employees_address")));
					B.setCity(Util.getStringValue(r.getAs("Employees_city")));
					B.setRegion(Util.getStringValue(r.getAs("Employees_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Employees_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Employees_country")));
					B.setHomePhone(Util.getStringValue(r.getAs("Employees_homePhone")));
					B.setExtension(Util.getStringValue(r.getAs("Employees_extension")));
					B.setPhoto(Util.getByteArrayValue(r.getAs("Employees_photo")));
					B.setNotes(Util.getStringValue(r.getAs("Employees_notes")));
					B.setPhotoPath(Util.getStringValue(r.getAs("Employees_photoPath")));
					B.setSalary(Util.getDoubleValue(r.getAs("Employees_salary")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Employees_logEvents")));
						
					res.setProcessedOrder(A);
					res.setEmployeeInCharge(B);
					return res;
				},Encoders.bean(Register.class)
			);
		
			datasetsPOJO.add(res_encoded);
		
			
			Dataset<Register> res_register_processedOrder;
			Dataset<Orders> res_Orders;
			
			
			//Join datasets or return 
			Dataset<Register> res = fullOuterJoinsRegister(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Orders> lonelyProcessedOrder = null;
			Dataset<Employees> lonelyEmployeeInCharge = null;
			
		
		
			
			if(processedOrder_refilter.booleanValue() || employeeInCharge_refilter.booleanValue())
				res = res.filter((FilterFunction<Register>) r -> (processedOrder_condition == null || processedOrder_condition.evaluate(r.getProcessedOrder())) && (employeeInCharge_condition == null || employeeInCharge_condition.evaluate(r.getEmployeeInCharge())));
			
		
			return res;
		
		}
	
	public Dataset<Register> getRegisterListByProcessedOrderCondition(
		Condition<OrdersAttribute> processedOrder_condition
	){
		return getRegisterList(processedOrder_condition, null);
	}
	
	public Register getRegisterByProcessedOrder(Orders processedOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, processedOrder.getId());
		Dataset<Register> res = getRegisterListByProcessedOrderCondition(cond);
		List<Register> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Register> getRegisterListByEmployeeInChargeCondition(
		Condition<EmployeesAttribute> employeeInCharge_condition
	){
		return getRegisterList(null, employeeInCharge_condition);
	}
	
	public Dataset<Register> getRegisterListByEmployeeInCharge(Employees employeeInCharge) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, employeeInCharge.getEmployeeID());
		Dataset<Register> res = getRegisterListByEmployeeInChargeCondition(cond);
	return res;
	}
	
	
	
	public void deleteRegisterList(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
			//TODO
		}
	
	public void deleteRegisterListByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteRegisterList(processedOrder_condition, null);
	}
	
	public void deleteRegisterByProcessedOrder(pojo.Orders processedOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteRegisterListByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteRegisterList(null, employeeInCharge_condition);
	}
	
	public void deleteRegisterListByEmployeeInCharge(pojo.Employees employeeInCharge) {
		// TODO using id for selecting
		return;
	}
		
}
