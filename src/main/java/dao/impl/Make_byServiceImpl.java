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
import conditions.Make_byAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrderTDO;
import tdo.Make_byTDO;
import conditions.OrderAttribute;
import dao.services.OrderService;
import tdo.CustomerTDO;
import tdo.Make_byTDO;
import conditions.CustomerAttribute;
import dao.services.CustomerService;
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

public class Make_byServiceImpl extends dao.services.Make_byService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Make_byServiceImpl.class);
	
	
	// Left side 'CustomerID' of reference [customerRef ]
	public Dataset<OrderTDO> getOrderTDOListOrderInCustomerRefInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
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
					
						// field  CustomerID for reference customerRef . Reference field : CustomerID
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null) {
						// Reference is in an Array of Embedded Objects
						order1.getMongoSchema_Orders_customerRef_CustomerID().add(nestedRow.getAs("CustomerID") == null ? null : nestedRow.getAs("CustomerID").toString());
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
	
	// Right side 'ID' of reference [customerRef ]
	public Dataset<CustomerTDO> getCustomerTDOListClientInCustomerRefInOrdersFromMongoSchema(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = CustomerServiceImpl.getBSONMatchQueryInCustomersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Customers", bsonQuery);
	
		Dataset<CustomerTDO> res = dataset.flatMap((FlatMapFunction<Row, CustomerTDO>) r -> {
				Set<CustomerTDO> list_res = new HashSet<CustomerTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				CustomerTDO customer1 = new CustomerTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Customer.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							customer1.setCity(null);
						}else{
							customer1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.companyName for field CompanyName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName") == null){
							customer1.setCompanyName(null);
						}else{
							customer1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.contactName for field ContactName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName") == null){
							customer1.setContactName(null);
						}else{
							customer1.setContactName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.contactTitle for field ContactTitle			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactTitle")) {
						if(nestedRow.getAs("ContactTitle") == null){
							customer1.setContactTitle(null);
						}else{
							customer1.setContactTitle(Util.getStringValue(nestedRow.getAs("ContactTitle")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							customer1.setCountry(null);
						}else{
							customer1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.fax for field Fax			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Fax")) {
						if(nestedRow.getAs("Fax") == null){
							customer1.setFax(null);
						}else{
							customer1.setFax(Util.getStringValue(nestedRow.getAs("Fax")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.id for field ID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ID")) {
						if(nestedRow.getAs("ID") == null){
							customer1.setId(null);
						}else{
							customer1.setId(Util.getStringValue(nestedRow.getAs("ID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.phone for field Phone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone") == null){
							customer1.setPhone(null);
						}else{
							customer1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							customer1.setPostalCode(null);
						}else{
							customer1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							customer1.setRegion(null);
						}else{
							customer1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							customer1.setAddress(null);
						}else{
							customer1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					
						// field  ID for reference customerRef . Reference field : ID
					nestedRow =  r1;
					if(nestedRow != null) {
						customer1.setMongoSchema_Orders_customerRef_ID(nestedRow.getAs("ID") == null ? null : nestedRow.getAs("ID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(customer1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(CustomerTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	
	public Dataset<Make_by> getMake_byList(
		Condition<OrderAttribute> order_condition,
		Condition<CustomerAttribute> client_condition){
			Make_byServiceImpl make_byService = this;
			OrderService orderService = new OrderServiceImpl();  
			CustomerService customerService = new CustomerServiceImpl();
			MutableBoolean order_refilter = new MutableBoolean(false);
			List<Dataset<Make_by>> datasetsPOJO = new ArrayList<Dataset<Make_by>>();
			boolean all_already_persisted = false;
			MutableBoolean client_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'order' in reference 'customerRef'. A->B Scenario
			client_refilter = new MutableBoolean(false);
			Dataset<OrderTDO> orderTDOcustomerReforder = make_byService.getOrderTDOListOrderInCustomerRefInOrdersFromMongoSchema(order_condition, order_refilter);
			Dataset<CustomerTDO> customerTDOcustomerRefclient = make_byService.getCustomerTDOListClientInCustomerRefInOrdersFromMongoSchema(client_condition, client_refilter);
			
			Dataset<Row> res_customerRef_temp = orderTDOcustomerReforder.join(customerTDOcustomerRefclient
					.withColumnRenamed("id", "Customer_id")
					.withColumnRenamed("city", "Customer_city")
					.withColumnRenamed("companyName", "Customer_companyName")
					.withColumnRenamed("contactName", "Customer_contactName")
					.withColumnRenamed("contactTitle", "Customer_contactTitle")
					.withColumnRenamed("country", "Customer_country")
					.withColumnRenamed("fax", "Customer_fax")
					.withColumnRenamed("phone", "Customer_phone")
					.withColumnRenamed("postalCode", "Customer_postalCode")
					.withColumnRenamed("region", "Customer_region")
					.withColumnRenamed("address", "Customer_address")
					.withColumnRenamed("logEvents", "Customer_logEvents"),
					functions.array_contains(orderTDOcustomerReforder.col("mongoSchema_Orders_customerRef_CustomerID"),customerTDOcustomerRefclient.col("mongoSchema_Orders_customerRef_ID")));
		
			Dataset<Make_by> res_customerRef = res_customerRef_temp.map(
				(MapFunction<Row, Make_by>) r -> {
					Make_by res = new Make_by();
					Order A = new Order();
					Customer B = new Customer();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setFreight(Util.getDoubleValue(r.getAs("freight")));
					A.setOrderDate(Util.getLocalDateValue(r.getAs("orderDate")));
					A.setRequiredDate(Util.getLocalDateValue(r.getAs("requiredDate")));
					A.setShipAddress(Util.getStringValue(r.getAs("shipAddress")));
					A.setShipCity(Util.getStringValue(r.getAs("shipCity")));
					A.setShipCountry(Util.getStringValue(r.getAs("shipCountry")));
					A.setShipName(Util.getStringValue(r.getAs("shipName")));
					A.setShipPostalCode(Util.getStringValue(r.getAs("shipPostalCode")));
					A.setShipRegion(Util.getStringValue(r.getAs("shipRegion")));
					A.setShippedDate(Util.getLocalDateValue(r.getAs("shippedDate")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getStringValue(r.getAs("Customer_id")));
					B.setCity(Util.getStringValue(r.getAs("Customer_city")));
					B.setCompanyName(Util.getStringValue(r.getAs("Customer_companyName")));
					B.setContactName(Util.getStringValue(r.getAs("Customer_contactName")));
					B.setContactTitle(Util.getStringValue(r.getAs("Customer_contactTitle")));
					B.setCountry(Util.getStringValue(r.getAs("Customer_country")));
					B.setFax(Util.getStringValue(r.getAs("Customer_fax")));
					B.setPhone(Util.getStringValue(r.getAs("Customer_phone")));
					B.setPostalCode(Util.getStringValue(r.getAs("Customer_postalCode")));
					B.setRegion(Util.getStringValue(r.getAs("Customer_region")));
					B.setAddress(Util.getStringValue(r.getAs("Customer_address")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Customer_logEvents")));
						
					res.setOrder(A);
					res.setClient(B);
					return res;
				},Encoders.bean(Make_by.class)
			);
		
			datasetsPOJO.add(res_customerRef);
		
			
			Dataset<Make_by> res_make_by_order;
			Dataset<Order> res_Order;
			
			
			//Join datasets or return 
			Dataset<Make_by> res = fullOuterJoinsMake_by(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyOrder = null;
			Dataset<Customer> lonelyClient = null;
			
		
		
			
			if(order_refilter.booleanValue() || client_refilter.booleanValue())
				res = res.filter((FilterFunction<Make_by>) r -> (order_condition == null || order_condition.evaluate(r.getOrder())) && (client_condition == null || client_condition.evaluate(r.getClient())));
			
		
			return res;
		
		}
	
	public Dataset<Make_by> getMake_byListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getMake_byList(order_condition, null);
	}
	
	public Make_by getMake_byByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Make_by> res = getMake_byListByOrderCondition(cond);
		List<Make_by> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Make_by> getMake_byListByClientCondition(
		Condition<CustomerAttribute> client_condition
	){
		return getMake_byList(null, client_condition);
	}
	
	public Dataset<Make_by> getMake_byListByClient(Customer client) {
		Condition<CustomerAttribute> cond = null;
		cond = Condition.simple(CustomerAttribute.id, Operator.EQUALS, client.getId());
		Dataset<Make_by> res = getMake_byListByClientCondition(cond);
	return res;
	}
	
	
	
	public void deleteMake_byList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
		}
	
	public void deleteMake_byListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteMake_byList(order_condition, null);
	}
	
	public void deleteMake_byByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteMake_byListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteMake_byList(null, client_condition);
	}
	
	public void deleteMake_byListByClient(pojo.Customer client) {
		// TODO using id for selecting
		return;
	}
		
}
