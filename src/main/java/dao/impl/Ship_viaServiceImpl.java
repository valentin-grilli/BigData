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
import conditions.Ship_viaAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ShipperTDO;
import tdo.Ship_viaTDO;
import conditions.ShipperAttribute;
import dao.services.ShipperService;
import tdo.OrderTDO;
import tdo.Ship_viaTDO;
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

public class Ship_viaServiceImpl extends dao.services.Ship_viaService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Ship_viaServiceImpl.class);
	
	
	// Left side 'ShipVia' of reference [shipperRef ]
	public Dataset<OrderTDO> getOrderTDOListOrderInShipperRefInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
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
					
						// field  ShipVia for reference shipperRef . Reference field : ShipVia
					nestedRow =  r1;
					if(nestedRow != null) {
						order1.setMongoSchema_Orders_shipperRef_ShipVia(nestedRow.getAs("ShipVia") == null ? null : nestedRow.getAs("ShipVia").toString());
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
	
	// Right side 'ShipperID' of reference [shipperRef ]
	public Dataset<ShipperTDO> getShipperTDOListShipperInShipperRefInOrdersFromMongoSchema(Condition<ShipperAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ShipperServiceImpl.getSQLWhereClauseInShippersFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "Shippers", where);
		
	
		Dataset<ShipperTDO> res = d.map((MapFunction<Row, ShipperTDO>) r -> {
					ShipperTDO shipper_res = new ShipperTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Shipper.Id]
					Integer id = Util.getIntegerValue(r.getAs("ShipperID"));
					shipper_res.setId(id);
					
					// attribute [Shipper.CompanyName]
					String companyName = Util.getStringValue(r.getAs("CompanyName"));
					shipper_res.setCompanyName(companyName);
					
					// attribute [Shipper.Phone]
					String phone = Util.getStringValue(r.getAs("Phone"));
					shipper_res.setPhone(phone);
	
					// Get reference column [ShipperID ] for reference [shipperRef]
					String mongoSchema_Orders_shipperRef_ShipperID = r.getAs("ShipperID") == null ? null : r.getAs("ShipperID").toString();
					shipper_res.setMongoSchema_Orders_shipperRef_ShipperID(mongoSchema_Orders_shipperRef_ShipperID);
	
	
					return shipper_res;
				}, Encoders.bean(ShipperTDO.class));
	
	
		return res;}
	
	
	
	
	public Dataset<Ship_via> getShip_viaList(
		Condition<ShipperAttribute> shipper_condition,
		Condition<OrderAttribute> order_condition){
			Ship_viaServiceImpl ship_viaService = this;
			ShipperService shipperService = new ShipperServiceImpl();  
			OrderService orderService = new OrderServiceImpl();
			MutableBoolean shipper_refilter = new MutableBoolean(false);
			List<Dataset<Ship_via>> datasetsPOJO = new ArrayList<Dataset<Ship_via>>();
			boolean all_already_persisted = false;
			MutableBoolean order_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			order_refilter = new MutableBoolean(false);
			// For role 'order' in reference 'shipperRef'  B->A Scenario
			Dataset<OrderTDO> orderTDOshipperReforder = ship_viaService.getOrderTDOListOrderInShipperRefInOrdersFromMongoSchema(order_condition, order_refilter);
			Dataset<ShipperTDO> shipperTDOshipperRefshipper = ship_viaService.getShipperTDOListShipperInShipperRefInOrdersFromMongoSchema(shipper_condition, shipper_refilter);
			
			Dataset<Row> res_shipperRef_temp = 
				shipperTDOshipperRefshipper.join(orderTDOshipperReforder
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
					shipperTDOshipperRefshipper.col("mongoSchema_Orders_shipperRef_ShipperID").equalTo(orderTDOshipperReforder.col("mongoSchema_Orders_shipperRef_ShipVia")));
		
			Dataset<Ship_via> res_shipperRef = res_shipperRef_temp.map(
				(MapFunction<Row, Ship_via>) r -> {
					Ship_via res = new Ship_via();
					Shipper A = new Shipper();
					Order B = new Order();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setCompanyName(Util.getStringValue(r.getAs("companyName")));
					A.setPhone(Util.getStringValue(r.getAs("phone")));
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
						
					res.setShipper(A);
					res.setOrder(B);
					return res;
				},Encoders.bean(Ship_via.class)
			);
		
					
			datasetsPOJO.add(res_shipperRef);
			
			Dataset<Ship_via> res_ship_via_shipper;
			Dataset<Shipper> res_Shipper;
			
			
			//Join datasets or return 
			Dataset<Ship_via> res = fullOuterJoinsShip_via(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Shipper> lonelyShipper = null;
			Dataset<Order> lonelyOrder = null;
			
		
		
			
			if(shipper_refilter.booleanValue() || order_refilter.booleanValue())
				res = res.filter((FilterFunction<Ship_via>) r -> (shipper_condition == null || shipper_condition.evaluate(r.getShipper())) && (order_condition == null || order_condition.evaluate(r.getOrder())));
			
		
			return res;
		
		}
	
	public Dataset<Ship_via> getShip_viaListByShipperCondition(
		Condition<ShipperAttribute> shipper_condition
	){
		return getShip_viaList(shipper_condition, null);
	}
	
	public Dataset<Ship_via> getShip_viaListByShipper(Shipper shipper) {
		Condition<ShipperAttribute> cond = null;
		cond = Condition.simple(ShipperAttribute.id, Operator.EQUALS, shipper.getId());
		Dataset<Ship_via> res = getShip_viaListByShipperCondition(cond);
	return res;
	}
	public Dataset<Ship_via> getShip_viaListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getShip_viaList(null, order_condition);
	}
	
	public Ship_via getShip_viaByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Ship_via> res = getShip_viaListByOrderCondition(cond);
		List<Ship_via> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	
	
	public void deleteShip_viaList(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteShip_viaListByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition
	){
		deleteShip_viaList(shipper_condition, null);
	}
	
	public void deleteShip_viaListByShipper(pojo.Shipper shipper) {
		// TODO using id for selecting
		return;
	}
	public void deleteShip_viaListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteShip_viaList(null, order_condition);
	}
	
	public void deleteShip_viaByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
