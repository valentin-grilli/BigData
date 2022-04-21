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
	
	
	// method accessing the embedded object customer mapped to role client
	public Dataset<Make_by> getMake_byListInmongoSchemaOrderscustomer(Condition<CustomerAttribute> client_condition, Condition<OrderAttribute> order_condition, MutableBoolean client_refilter, MutableBoolean order_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(order_condition ,order_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = CustomerServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(client_condition ,client_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
		
			Dataset<Make_by> res = dataset.flatMap((FlatMapFunction<Row, Make_by>) r -> {
					List<Make_by> list_res = new ArrayList<Make_by>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Make_by make_by1 = new Make_by();
					make_by1.setOrder(new Order());
					make_by1.setClient(new Customer());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							make_by1.getOrder().setFreight(null);
						else{
							make_by1.getOrder().setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							make_by1.getOrder().setOrderDate(null);
						else{
							make_by1.getOrder().setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							make_by1.getOrder().setRequiredDate(null);
						else{
							make_by1.getOrder().setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							make_by1.getOrder().setShipAddress(null);
						else{
							make_by1.getOrder().setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							make_by1.getOrder().setId(null);
						else{
							make_by1.getOrder().setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							make_by1.getOrder().setShipCity(null);
						else{
							make_by1.getOrder().setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							make_by1.getOrder().setShipCountry(null);
						else{
							make_by1.getOrder().setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							make_by1.getOrder().setShipName(null);
						else{
							make_by1.getOrder().setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							make_by1.getOrder().setShipPostalCode(null);
						else{
							make_by1.getOrder().setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							make_by1.getOrder().setShipRegion(null);
						else{
							make_by1.getOrder().setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							make_by1.getOrder().setShippedDate(null);
						else{
							make_by1.getOrder().setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.id for field CustomerID			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CustomerID")) {
						if(nestedRow.getAs("CustomerID")==null)
							make_by1.getClient().setId(null);
						else{
							make_by1.getClient().setId(Util.getStringValue(nestedRow.getAs("CustomerID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.companyName for field ContactName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName")==null)
							make_by1.getClient().setCompanyName(null);
						else{
							make_by1.getClient().setCompanyName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					if(toAdd1 ) {
						if(!(make_by1.getOrder().equals(new Order())) && !(make_by1.getClient().equals(new Customer())))
							list_res.add(make_by1);
						addedInList = true;
					} 
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Make_by.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
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
		
			
			Dataset<Make_by> res_make_by_order;
			Dataset<Order> res_Order;
			// Role 'client' mapped to EmbeddedObject 'customer' 'Order' containing 'Customer' 
			client_refilter = new MutableBoolean(false);
			res_make_by_order = make_byService.getMake_byListInmongoSchemaOrderscustomer(client_condition, order_condition, client_refilter, order_refilter);
			
			datasetsPOJO.add(res_make_by_order);
			
			
			//Join datasets or return 
			Dataset<Make_by> res = fullOuterJoinsMake_by(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyOrder = null;
			Dataset<Customer> lonelyClient = null;
			
		
			List<Dataset<Customer>> lonelyclientList = new ArrayList<Dataset<Customer>>();
			lonelyclientList.add(customerService.getCustomerListInCustomersFromMyMongoDB(client_condition, new MutableBoolean(false)));
			lonelyClient = CustomerService.fullOuterJoinsCustomer(lonelyclientList);
			if(lonelyClient != null) {
				res = fullLeftOuterJoinBetweenMake_byAndClient(res, lonelyClient);
			}	
		
			
			if(order_refilter.booleanValue() || client_refilter.booleanValue())
				res = res.filter((FilterFunction<Make_by>) r -> (order_condition == null || order_condition.evaluate(r.getOrder())) && (client_condition == null || client_condition.evaluate(r.getClient())));
			
		
			return res;
		
		}
	
	public Dataset<Make_by> getMake_byListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getMake_byList(order_condition, null);
	}
	
	public Dataset<Make_by> getMake_byListByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Make_by> res = getMake_byListByOrderCondition(cond);
	return res;
	}
	public Dataset<Make_by> getMake_byListByClientCondition(
		Condition<CustomerAttribute> client_condition
	){
		return getMake_byList(null, client_condition);
	}
	
	public Make_by getMake_byByClient(Customer client) {
		Condition<CustomerAttribute> cond = null;
		cond = Condition.simple(CustomerAttribute.id, Operator.EQUALS, client.getId());
		Dataset<Make_by> res = getMake_byListByClientCondition(cond);
		List<Make_by> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
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
	
	public void deleteMake_byListByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteMake_byListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteMake_byList(null, client_condition);
	}
	
	public void deleteMake_byByClient(pojo.Customer client) {
		// TODO using id for selecting
		return;
	}
		
}
