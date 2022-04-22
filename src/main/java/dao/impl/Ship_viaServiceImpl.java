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
		
			
			Dataset<Ship_via> res_ship_via_shipper;
			Dataset<Shipper> res_Shipper;
			
			
			//Join datasets or return 
			Dataset<Ship_via> res = fullOuterJoinsShip_via(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Shipper> lonelyShipper = null;
			Dataset<Order> lonelyOrder = null;
			
			List<Dataset<Shipper>> lonelyshipperList = new ArrayList<Dataset<Shipper>>();
			lonelyshipperList.add(shipperService.getShipperListInShippersFromRelData(shipper_condition, new MutableBoolean(false)));
			lonelyShipper = ShipperService.fullOuterJoinsShipper(lonelyshipperList);
			if(lonelyShipper != null) {
				res = fullLeftOuterJoinBetweenShip_viaAndShipper(res, lonelyShipper);
			}	
		
			List<Dataset<Order>> lonelyorderList = new ArrayList<Dataset<Order>>();
			lonelyorderList.add(orderService.getOrderListInOrdersFromMyMongoDB(order_condition, new MutableBoolean(false)));
			lonelyOrder = OrderService.fullOuterJoinsOrder(lonelyorderList);
			if(lonelyOrder != null) {
				res = fullLeftOuterJoinBetweenShip_viaAndOrder(res, lonelyOrder);
			}	
		
			
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
