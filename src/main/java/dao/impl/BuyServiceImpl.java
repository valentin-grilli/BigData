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
import conditions.BuyAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.BuyTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.CustomersTDO;
import tdo.BuyTDO;
import conditions.CustomersAttribute;
import dao.services.CustomersService;
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

public class BuyServiceImpl extends dao.services.BuyService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuyServiceImpl.class);
	
	// method accessing the embedded object customer mapped to role boughtOrder
	public Dataset<Buy> getBuyListInmongoDBOrderscustomer(Condition<OrdersAttribute> boughtOrder_condition, Condition<CustomersAttribute> customer_condition, MutableBoolean boughtOrder_refilter, MutableBoolean customer_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(boughtOrder_condition ,boughtOrder_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = CustomersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(customer_condition ,customer_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
		
			Dataset<Buy> res = dataset.flatMap((FlatMapFunction<Row, Buy>) r -> {
					List<Buy> list_res = new ArrayList<Buy>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Buy buy1 = new Buy();
					buy1.setBoughtOrder(new Orders());
					buy1.setCustomer(new Customers());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							buy1.getBoughtOrder().setId(null);
						else{
							buy1.getBoughtOrder().setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							buy1.getBoughtOrder().setOrderDate(null);
						else{
							buy1.getBoughtOrder().setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							buy1.getBoughtOrder().setRequiredDate(null);
						else{
							buy1.getBoughtOrder().setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							buy1.getBoughtOrder().setShippedDate(null);
						else{
							buy1.getBoughtOrder().setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							buy1.getBoughtOrder().setFreight(null);
						else{
							buy1.getBoughtOrder().setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							buy1.getBoughtOrder().setShipName(null);
						else{
							buy1.getBoughtOrder().setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							buy1.getBoughtOrder().setShipAddress(null);
						else{
							buy1.getBoughtOrder().setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							buy1.getBoughtOrder().setShipCity(null);
						else{
							buy1.getBoughtOrder().setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							buy1.getBoughtOrder().setShipRegion(null);
						else{
							buy1.getBoughtOrder().setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							buy1.getBoughtOrder().setShipPostalCode(null);
						else{
							buy1.getBoughtOrder().setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							buy1.getBoughtOrder().setShipCountry(null);
						else{
							buy1.getBoughtOrder().setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customers.customerID for field CustomerID			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CustomerID")) {
						if(nestedRow.getAs("CustomerID")==null)
							buy1.getCustomer().setCustomerID(null);
						else{
							buy1.getCustomer().setCustomerID(Util.getStringValue(nestedRow.getAs("CustomerID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customers.contactName for field ContactName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName")==null)
							buy1.getCustomer().setContactName(null);
						else{
							buy1.getCustomer().setContactName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					if(toAdd1 ) {
						if(!(buy1.getBoughtOrder().equals(new Orders())) && !(buy1.getCustomer().equals(new Customers())))
							list_res.add(buy1);
						addedInList = true;
					} 
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Buy.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	public Dataset<Buy> getBuyList(
		Condition<OrdersAttribute> boughtOrder_condition,
		Condition<CustomersAttribute> customer_condition){
			BuyServiceImpl buyService = this;
			OrdersService ordersService = new OrdersServiceImpl();  
			CustomersService customersService = new CustomersServiceImpl();
			MutableBoolean boughtOrder_refilter = new MutableBoolean(false);
			List<Dataset<Buy>> datasetsPOJO = new ArrayList<Dataset<Buy>>();
			boolean all_already_persisted = false;
			MutableBoolean customer_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Buy> res_buy_boughtOrder;
			Dataset<Orders> res_Orders;
			// Role 'boughtOrder' mapped to EmbeddedObject 'customer' - 'Customers' containing 'Orders'
			customer_refilter = new MutableBoolean(false);
			res_buy_boughtOrder = buyService.getBuyListInmongoDBOrderscustomer(boughtOrder_condition, customer_condition, boughtOrder_refilter, customer_refilter);
		 	
			datasetsPOJO.add(res_buy_boughtOrder);
			
			
			//Join datasets or return 
			Dataset<Buy> res = fullOuterJoinsBuy(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Orders> lonelyBoughtOrder = null;
			Dataset<Customers> lonelyCustomer = null;
			
		
			List<Dataset<Customers>> lonelycustomerList = new ArrayList<Dataset<Customers>>();
			lonelycustomerList.add(customersService.getCustomersListInCustomersFromMyMongoDB(customer_condition, new MutableBoolean(false)));
			lonelyCustomer = CustomersService.fullOuterJoinsCustomers(lonelycustomerList);
			if(lonelyCustomer != null) {
				res = fullLeftOuterJoinBetweenBuyAndCustomer(res, lonelyCustomer);
			}	
		
			
			if(boughtOrder_refilter.booleanValue() || customer_refilter.booleanValue())
				res = res.filter((FilterFunction<Buy>) r -> (boughtOrder_condition == null || boughtOrder_condition.evaluate(r.getBoughtOrder())) && (customer_condition == null || customer_condition.evaluate(r.getCustomer())));
			
		
			return res;
		
		}
	
	public Dataset<Buy> getBuyListByBoughtOrderCondition(
		Condition<OrdersAttribute> boughtOrder_condition
	){
		return getBuyList(boughtOrder_condition, null);
	}
	
	public Buy getBuyByBoughtOrder(Orders boughtOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, boughtOrder.getId());
		Dataset<Buy> res = getBuyListByBoughtOrderCondition(cond);
		List<Buy> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Buy> getBuyListByCustomerCondition(
		Condition<CustomersAttribute> customer_condition
	){
		return getBuyList(null, customer_condition);
	}
	
	public Dataset<Buy> getBuyListByCustomer(Customers customer) {
		Condition<CustomersAttribute> cond = null;
		cond = Condition.simple(CustomersAttribute.customerID, Operator.EQUALS, customer.getCustomerID());
		Dataset<Buy> res = getBuyListByCustomerCondition(cond);
	return res;
	}
	
	
	
	public void deleteBuyList(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition){
			//TODO
		}
	
	public void deleteBuyListByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteBuyList(boughtOrder_condition, null);
	}
	
	public void deleteBuyByBoughtOrder(pojo.Orders boughtOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteBuyListByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteBuyList(null, customer_condition);
	}
	
	public void deleteBuyListByCustomer(pojo.Customers customer) {
		// TODO using id for selecting
		return;
	}
		
}
