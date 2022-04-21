package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Order;
import conditions.OrderAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.OrderAttribute;
import pojo.Make_by;
import conditions.CustomerAttribute;
import pojo.Customer;

public abstract class OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderService.class);
	protected Make_byService make_byService = new dao.impl.Make_byServiceImpl();
	


	public static enum ROLE_NAME {
		MAKE_BY_ORDER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.MAKE_BY_ORDER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public OrderService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public OrderService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Order> getOrderList(){
		return getOrderList(null);
	}
	
	public Dataset<Order> getOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Order>> datasets = new ArrayList<Dataset<Order>>();
		Dataset<Order> d = null;
		d = getOrderListInOrdersFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsOrder(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Order>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Order> getOrderListInOrdersFromMyMongoDB(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Order getOrderById(Integer id){
		Condition cond;
		cond = Condition.simple(OrderAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Order> res = getOrderList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Order> getOrderListById(Integer id) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Order> getOrderListByFreight(Double freight) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.freight, conditions.Operator.EQUALS, freight));
	}
	
	public Dataset<Order> getOrderListByOrderDate(LocalDate orderDate) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.orderDate, conditions.Operator.EQUALS, orderDate));
	}
	
	public Dataset<Order> getOrderListByRequiredDate(LocalDate requiredDate) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.requiredDate, conditions.Operator.EQUALS, requiredDate));
	}
	
	public Dataset<Order> getOrderListByShipAddress(String shipAddress) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shipAddress, conditions.Operator.EQUALS, shipAddress));
	}
	
	public Dataset<Order> getOrderListByShipCity(String shipCity) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shipCity, conditions.Operator.EQUALS, shipCity));
	}
	
	public Dataset<Order> getOrderListByShipCountry(String shipCountry) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shipCountry, conditions.Operator.EQUALS, shipCountry));
	}
	
	public Dataset<Order> getOrderListByShipName(String shipName) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shipName, conditions.Operator.EQUALS, shipName));
	}
	
	public Dataset<Order> getOrderListByShipPostalCode(String shipPostalCode) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shipPostalCode, conditions.Operator.EQUALS, shipPostalCode));
	}
	
	public Dataset<Order> getOrderListByShipRegion(String shipRegion) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shipRegion, conditions.Operator.EQUALS, shipRegion));
	}
	
	public Dataset<Order> getOrderListByShippedDate(LocalDate shippedDate) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.shippedDate, conditions.Operator.EQUALS, shippedDate));
	}
	
	
	
	public static Dataset<Order> fullOuterJoinsOrder(List<Dataset<Order>> datasetsPOJO) {
		return fullOuterJoinsOrder(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Order> fullLeftOuterJoinsOrder(List<Dataset<Order>> datasetsPOJO) {
		return fullOuterJoinsOrder(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Order> fullOuterJoinsOrder(List<Dataset<Order>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Order> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Order] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("freight", "freight_1")
								.withColumnRenamed("orderDate", "orderDate_1")
								.withColumnRenamed("requiredDate", "requiredDate_1")
								.withColumnRenamed("shipAddress", "shipAddress_1")
								.withColumnRenamed("shipCity", "shipCity_1")
								.withColumnRenamed("shipCountry", "shipCountry_1")
								.withColumnRenamed("shipName", "shipName_1")
								.withColumnRenamed("shipPostalCode", "shipPostalCode_1")
								.withColumnRenamed("shipRegion", "shipRegion_1")
								.withColumnRenamed("shippedDate", "shippedDate_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("freight", "freight_" + i)
								.withColumnRenamed("orderDate", "orderDate_" + i)
								.withColumnRenamed("requiredDate", "requiredDate_" + i)
								.withColumnRenamed("shipAddress", "shipAddress_" + i)
								.withColumnRenamed("shipCity", "shipCity_" + i)
								.withColumnRenamed("shipCountry", "shipCountry_" + i)
								.withColumnRenamed("shipName", "shipName_" + i)
								.withColumnRenamed("shipPostalCode", "shipPostalCode_" + i)
								.withColumnRenamed("shipRegion", "shipRegion_" + i)
								.withColumnRenamed("shippedDate", "shippedDate_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Order] objects"); 
			d = res.map((MapFunction<Row, Order>) r -> {
					Order order_res = new Order();
					
					// attribute 'Order.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					order_res.setId(firstNotNull_id);
					
					// attribute 'Order.freight'
					Double firstNotNull_freight = Util.getDoubleValue(r.getAs("freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double freight2 = Util.getDoubleValue(r.getAs("freight_" + i));
						if (firstNotNull_freight != null && freight2 != null && !firstNotNull_freight.equals(freight2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_freight + " and " + freight2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_freight + " and " + freight2 + "." );
						}
						if (firstNotNull_freight == null && freight2 != null) {
							firstNotNull_freight = freight2;
						}
					}
					order_res.setFreight(firstNotNull_freight);
					
					// attribute 'Order.orderDate'
					LocalDate firstNotNull_orderDate = Util.getLocalDateValue(r.getAs("orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate orderDate2 = Util.getLocalDateValue(r.getAs("orderDate_" + i));
						if (firstNotNull_orderDate != null && orderDate2 != null && !firstNotNull_orderDate.equals(orderDate2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_orderDate + " and " + orderDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_orderDate + " and " + orderDate2 + "." );
						}
						if (firstNotNull_orderDate == null && orderDate2 != null) {
							firstNotNull_orderDate = orderDate2;
						}
					}
					order_res.setOrderDate(firstNotNull_orderDate);
					
					// attribute 'Order.requiredDate'
					LocalDate firstNotNull_requiredDate = Util.getLocalDateValue(r.getAs("requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate requiredDate2 = Util.getLocalDateValue(r.getAs("requiredDate_" + i));
						if (firstNotNull_requiredDate != null && requiredDate2 != null && !firstNotNull_requiredDate.equals(requiredDate2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_requiredDate + " and " + requiredDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_requiredDate + " and " + requiredDate2 + "." );
						}
						if (firstNotNull_requiredDate == null && requiredDate2 != null) {
							firstNotNull_requiredDate = requiredDate2;
						}
					}
					order_res.setRequiredDate(firstNotNull_requiredDate);
					
					// attribute 'Order.shipAddress'
					String firstNotNull_shipAddress = Util.getStringValue(r.getAs("shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipAddress2 = Util.getStringValue(r.getAs("shipAddress_" + i));
						if (firstNotNull_shipAddress != null && shipAddress2 != null && !firstNotNull_shipAddress.equals(shipAddress2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_shipAddress + " and " + shipAddress2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_shipAddress + " and " + shipAddress2 + "." );
						}
						if (firstNotNull_shipAddress == null && shipAddress2 != null) {
							firstNotNull_shipAddress = shipAddress2;
						}
					}
					order_res.setShipAddress(firstNotNull_shipAddress);
					
					// attribute 'Order.shipCity'
					String firstNotNull_shipCity = Util.getStringValue(r.getAs("shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipCity2 = Util.getStringValue(r.getAs("shipCity_" + i));
						if (firstNotNull_shipCity != null && shipCity2 != null && !firstNotNull_shipCity.equals(shipCity2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_shipCity + " and " + shipCity2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_shipCity + " and " + shipCity2 + "." );
						}
						if (firstNotNull_shipCity == null && shipCity2 != null) {
							firstNotNull_shipCity = shipCity2;
						}
					}
					order_res.setShipCity(firstNotNull_shipCity);
					
					// attribute 'Order.shipCountry'
					String firstNotNull_shipCountry = Util.getStringValue(r.getAs("shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipCountry2 = Util.getStringValue(r.getAs("shipCountry_" + i));
						if (firstNotNull_shipCountry != null && shipCountry2 != null && !firstNotNull_shipCountry.equals(shipCountry2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_shipCountry + " and " + shipCountry2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_shipCountry + " and " + shipCountry2 + "." );
						}
						if (firstNotNull_shipCountry == null && shipCountry2 != null) {
							firstNotNull_shipCountry = shipCountry2;
						}
					}
					order_res.setShipCountry(firstNotNull_shipCountry);
					
					// attribute 'Order.shipName'
					String firstNotNull_shipName = Util.getStringValue(r.getAs("shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipName2 = Util.getStringValue(r.getAs("shipName_" + i));
						if (firstNotNull_shipName != null && shipName2 != null && !firstNotNull_shipName.equals(shipName2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_shipName + " and " + shipName2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_shipName + " and " + shipName2 + "." );
						}
						if (firstNotNull_shipName == null && shipName2 != null) {
							firstNotNull_shipName = shipName2;
						}
					}
					order_res.setShipName(firstNotNull_shipName);
					
					// attribute 'Order.shipPostalCode'
					String firstNotNull_shipPostalCode = Util.getStringValue(r.getAs("shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipPostalCode2 = Util.getStringValue(r.getAs("shipPostalCode_" + i));
						if (firstNotNull_shipPostalCode != null && shipPostalCode2 != null && !firstNotNull_shipPostalCode.equals(shipPostalCode2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_shipPostalCode + " and " + shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_shipPostalCode + " and " + shipPostalCode2 + "." );
						}
						if (firstNotNull_shipPostalCode == null && shipPostalCode2 != null) {
							firstNotNull_shipPostalCode = shipPostalCode2;
						}
					}
					order_res.setShipPostalCode(firstNotNull_shipPostalCode);
					
					// attribute 'Order.shipRegion'
					String firstNotNull_shipRegion = Util.getStringValue(r.getAs("shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipRegion2 = Util.getStringValue(r.getAs("shipRegion_" + i));
						if (firstNotNull_shipRegion != null && shipRegion2 != null && !firstNotNull_shipRegion.equals(shipRegion2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_shipRegion + " and " + shipRegion2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_shipRegion + " and " + shipRegion2 + "." );
						}
						if (firstNotNull_shipRegion == null && shipRegion2 != null) {
							firstNotNull_shipRegion = shipRegion2;
						}
					}
					order_res.setShipRegion(firstNotNull_shipRegion);
					
					// attribute 'Order.shippedDate'
					LocalDate firstNotNull_shippedDate = Util.getLocalDateValue(r.getAs("shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate shippedDate2 = Util.getLocalDateValue(r.getAs("shippedDate_" + i));
						if (firstNotNull_shippedDate != null && shippedDate2 != null && !firstNotNull_shippedDate.equals(shippedDate2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_shippedDate + " and " + shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_shippedDate + " and " + shippedDate2 + "." );
						}
						if (firstNotNull_shippedDate == null && shippedDate2 != null) {
							firstNotNull_shippedDate = shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_shippedDate);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							order_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							order_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return order_res;
				}, Encoders.bean(Order.class));
			return d;
	}
	
	
	public Order getOrder(Order.make_by role, Customer customer) {
		if(role != null) {
			if(role.equals(Order.make_by.order))
				return getOrderInMake_byByClient(customer);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.make_by role, Condition<CustomerAttribute> condition) {
		if(role != null) {
			if(role.equals(Order.make_by.order))
				return getOrderListInMake_byByClientCondition(condition);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.make_by role, Condition<OrderAttribute> condition1, Condition<CustomerAttribute> condition2) {
		if(role != null) {
			if(role.equals(Order.make_by.order))
				return getOrderListInMake_by(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Order> getOrderListInMake_by(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public Dataset<Order> getOrderListInMake_byByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInMake_by(order_condition, null);
	}
	public Dataset<Order> getOrderListInMake_byByClientCondition(conditions.Condition<conditions.CustomerAttribute> client_condition){
		return getOrderListInMake_by(null, client_condition);
	}
	
	public Order getOrderInMake_byByClient(pojo.Customer client){
		if(client == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, client.getId());
		Dataset<Order> res = getOrderListInMake_byByClientCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	
	public abstract boolean insertOrder(Order order);
	
	private boolean inUpdateMethod = false;
	private List<Row> allOrderIdList = null;
	public abstract void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set);
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public abstract void updateOrderListInMake_by(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	);
	
	public void updateOrderListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInMake_by(order_condition, null, set);
	}
	public void updateOrderListInMake_byByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInMake_by(null, client_condition, set);
	}
	
	public void updateOrderInMake_byByClient(
		pojo.Customer client,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition);
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public abstract void deleteOrderListInMake_by(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public void deleteOrderListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInMake_by(order_condition, null);
	}
	public void deleteOrderListInMake_byByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteOrderListInMake_by(null, client_condition);
	}
	
	public void deleteOrderInMake_byByClient(
		pojo.Customer client 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
