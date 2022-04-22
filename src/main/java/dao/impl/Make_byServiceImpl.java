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
			
			
			//Join datasets or return 
			Dataset<Make_by> res = fullOuterJoinsMake_by(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyOrder = null;
			Dataset<Customer> lonelyClient = null;
			
			List<Dataset<Order>> lonelyorderList = new ArrayList<Dataset<Order>>();
			lonelyorderList.add(orderService.getOrderListInOrdersFromMyMongoDB(order_condition, new MutableBoolean(false)));
			lonelyOrder = OrderService.fullOuterJoinsOrder(lonelyorderList);
			if(lonelyOrder != null) {
				res = fullLeftOuterJoinBetweenMake_byAndOrder(res, lonelyOrder);
			}	
		
			List<Dataset<Customer>> lonelyclientList = new ArrayList<Dataset<Customer>>();
			lonelyclientList.add(customerService.getCustomerListInCustomersFromMyMongoDB(client_condition, new MutableBoolean(false)));
			lonelyclientList.add(customerService.getCustomerListInOrdersFromMyMongoDB(client_condition, new MutableBoolean(false)));
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
