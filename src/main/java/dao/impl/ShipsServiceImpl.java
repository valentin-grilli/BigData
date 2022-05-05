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
import conditions.ShipsAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.ShipsTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.ShippersTDO;
import tdo.ShipsTDO;
import conditions.ShippersAttribute;
import dao.services.ShippersService;
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

public class ShipsServiceImpl extends dao.services.ShipsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShipsServiceImpl.class);
	
	
	// Left side 'ShipVia' of reference [deliver ]
	public Dataset<OrdersTDO> getOrdersTDOListShippedOrderInDeliverInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag){	
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
					
						// field  ShipVia for reference deliver . Reference field : ShipVia
					nestedRow =  r1;
					if(nestedRow != null) {
						orders1.setMongoDB_Orders_deliver_ShipVia(nestedRow.getAs("ShipVia") == null ? null : nestedRow.getAs("ShipVia").toString());
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
	
	// Right side 'ShipperID' of reference [deliver ]
	public Dataset<ShippersTDO> getShippersTDOListShipperInDeliverInOrdersFromMongoDB(Condition<ShippersAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ShippersServiceImpl.getSQLWhereClauseInShippersFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Shippers", where);
		
	
		Dataset<ShippersTDO> res = d.map((MapFunction<Row, ShippersTDO>) r -> {
					ShippersTDO shippers_res = new ShippersTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Shippers.ShipperID]
					Integer shipperID = Util.getIntegerValue(r.getAs("ShipperID"));
					shippers_res.setShipperID(shipperID);
					
					// attribute [Shippers.CompanyName]
					String companyName = Util.getStringValue(r.getAs("CompanyName"));
					shippers_res.setCompanyName(companyName);
					
					// attribute [Shippers.Phone]
					String phone = Util.getStringValue(r.getAs("Phone"));
					shippers_res.setPhone(phone);
	
					// Get reference column [ShipperID ] for reference [deliver]
					String mongoDB_Orders_deliver_ShipperID = r.getAs("ShipperID") == null ? null : r.getAs("ShipperID").toString();
					shippers_res.setMongoDB_Orders_deliver_ShipperID(mongoDB_Orders_deliver_ShipperID);
	
	
					return shippers_res;
				}, Encoders.bean(ShippersTDO.class));
	
	
		return res;}
	
	
	
	
	public Dataset<Ships> getShipsList(
		Condition<OrdersAttribute> shippedOrder_condition,
		Condition<ShippersAttribute> shipper_condition){
			ShipsServiceImpl shipsService = this;
			OrdersService ordersService = new OrdersServiceImpl();  
			ShippersService shippersService = new ShippersServiceImpl();
			MutableBoolean shippedOrder_refilter = new MutableBoolean(false);
			List<Dataset<Ships>> datasetsPOJO = new ArrayList<Dataset<Ships>>();
			boolean all_already_persisted = false;
			MutableBoolean shipper_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'shippedOrder' in reference 'deliver'. A->B Scenario
			shipper_refilter = new MutableBoolean(false);
			Dataset<OrdersTDO> ordersTDOdelivershippedOrder = shipsService.getOrdersTDOListShippedOrderInDeliverInOrdersFromMongoDB(shippedOrder_condition, shippedOrder_refilter);
			Dataset<ShippersTDO> shippersTDOdelivershipper = shipsService.getShippersTDOListShipperInDeliverInOrdersFromMongoDB(shipper_condition, shipper_refilter);
			
			Dataset<Row> res_deliver_temp = ordersTDOdelivershippedOrder.join(shippersTDOdelivershipper
					.withColumnRenamed("shipperID", "Shippers_shipperID")
					.withColumnRenamed("companyName", "Shippers_companyName")
					.withColumnRenamed("phone", "Shippers_phone")
					.withColumnRenamed("logEvents", "Shippers_logEvents"),
					ordersTDOdelivershippedOrder.col("mongoDB_Orders_deliver_ShipVia").equalTo(shippersTDOdelivershipper.col("mongoDB_Orders_deliver_ShipperID")));
		
			Dataset<Ships> res_deliver = res_deliver_temp.map(
				(MapFunction<Row, Ships>) r -> {
					Ships res = new Ships();
					Orders A = new Orders();
					Shippers B = new Shippers();
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
		
					B.setShipperID(Util.getIntegerValue(r.getAs("Shippers_shipperID")));
					B.setCompanyName(Util.getStringValue(r.getAs("Shippers_companyName")));
					B.setPhone(Util.getStringValue(r.getAs("Shippers_phone")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Shippers_logEvents")));
						
					res.setShippedOrder(A);
					res.setShipper(B);
					return res;
				},Encoders.bean(Ships.class)
			);
		
			datasetsPOJO.add(res_deliver);
		
			
			Dataset<Ships> res_ships_shippedOrder;
			Dataset<Orders> res_Orders;
			
			
			//Join datasets or return 
			Dataset<Ships> res = fullOuterJoinsShips(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Orders> lonelyShippedOrder = null;
			Dataset<Shippers> lonelyShipper = null;
			
		
		
			
			if(shippedOrder_refilter.booleanValue() || shipper_refilter.booleanValue())
				res = res.filter((FilterFunction<Ships>) r -> (shippedOrder_condition == null || shippedOrder_condition.evaluate(r.getShippedOrder())) && (shipper_condition == null || shipper_condition.evaluate(r.getShipper())));
			
		
			return res;
		
		}
	
	public Dataset<Ships> getShipsListByShippedOrderCondition(
		Condition<OrdersAttribute> shippedOrder_condition
	){
		return getShipsList(shippedOrder_condition, null);
	}
	
	public Ships getShipsByShippedOrder(Orders shippedOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, shippedOrder.getId());
		Dataset<Ships> res = getShipsListByShippedOrderCondition(cond);
		List<Ships> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Ships> getShipsListByShipperCondition(
		Condition<ShippersAttribute> shipper_condition
	){
		return getShipsList(null, shipper_condition);
	}
	
	public Dataset<Ships> getShipsListByShipper(Shippers shipper) {
		Condition<ShippersAttribute> cond = null;
		cond = Condition.simple(ShippersAttribute.shipperID, Operator.EQUALS, shipper.getShipperID());
		Dataset<Ships> res = getShipsListByShipperCondition(cond);
	return res;
	}
	
	
	
	public void deleteShipsList(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition){
			//TODO
		}
	
	public void deleteShipsListByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShipsList(shippedOrder_condition, null);
	}
	
	public void deleteShipsByShippedOrder(pojo.Orders shippedOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteShipsListByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShipsList(null, shipper_condition);
	}
	
	public void deleteShipsListByShipper(pojo.Shippers shipper) {
		// TODO using id for selecting
		return;
	}
		
}
