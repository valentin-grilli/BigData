package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Shipper;
import conditions.ShipperAttribute;
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
import conditions.ShipperAttribute;
import pojo.Ship_via;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class ShipperService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShipperService.class);
	protected Ship_viaService ship_viaService = new dao.impl.Ship_viaServiceImpl();
	


	public static enum ROLE_NAME {
		SHIP_VIA_SHIPPER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.SHIP_VIA_SHIPPER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ShipperService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ShipperService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Shipper> getShipperList(){
		return getShipperList(null);
	}
	
	public Dataset<Shipper> getShipperList(conditions.Condition<conditions.ShipperAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Shipper>> datasets = new ArrayList<Dataset<Shipper>>();
		Dataset<Shipper> d = null;
		d = getShipperListInShippersFromRelData(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsShipper(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Shipper>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Shipper> getShipperListInShippersFromRelData(conditions.Condition<conditions.ShipperAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Shipper getShipperById(Integer id){
		Condition cond;
		cond = Condition.simple(ShipperAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Shipper> res = getShipperList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Shipper> getShipperListById(Integer id) {
		return getShipperList(conditions.Condition.simple(conditions.ShipperAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Shipper> getShipperListByCompanyName(String companyName) {
		return getShipperList(conditions.Condition.simple(conditions.ShipperAttribute.companyName, conditions.Operator.EQUALS, companyName));
	}
	
	public Dataset<Shipper> getShipperListByPhone(String phone) {
		return getShipperList(conditions.Condition.simple(conditions.ShipperAttribute.phone, conditions.Operator.EQUALS, phone));
	}
	
	
	
	public static Dataset<Shipper> fullOuterJoinsShipper(List<Dataset<Shipper>> datasetsPOJO) {
		return fullOuterJoinsShipper(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Shipper> fullLeftOuterJoinsShipper(List<Dataset<Shipper>> datasetsPOJO) {
		return fullOuterJoinsShipper(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Shipper> fullOuterJoinsShipper(List<Dataset<Shipper>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Shipper> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Shipper] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("companyName", "companyName_1")
								.withColumnRenamed("phone", "phone_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("companyName", "companyName_" + i)
								.withColumnRenamed("phone", "phone_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Shipper] objects"); 
			d = res.map((MapFunction<Row, Shipper>) r -> {
					Shipper shipper_res = new Shipper();
					
					// attribute 'Shipper.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					shipper_res.setId(firstNotNull_id);
					
					// attribute 'Shipper.companyName'
					String firstNotNull_companyName = Util.getStringValue(r.getAs("companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String companyName2 = Util.getStringValue(r.getAs("companyName_" + i));
						if (firstNotNull_companyName != null && companyName2 != null && !firstNotNull_companyName.equals(companyName2)) {
							shipper_res.addLogEvent("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
							logger.warn("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
						}
						if (firstNotNull_companyName == null && companyName2 != null) {
							firstNotNull_companyName = companyName2;
						}
					}
					shipper_res.setCompanyName(firstNotNull_companyName);
					
					// attribute 'Shipper.phone'
					String firstNotNull_phone = Util.getStringValue(r.getAs("phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String phone2 = Util.getStringValue(r.getAs("phone_" + i));
						if (firstNotNull_phone != null && phone2 != null && !firstNotNull_phone.equals(phone2)) {
							shipper_res.addLogEvent("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.phone': " + firstNotNull_phone + " and " + phone2 + "." );
							logger.warn("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.phone': " + firstNotNull_phone + " and " + phone2 + "." );
						}
						if (firstNotNull_phone == null && phone2 != null) {
							firstNotNull_phone = phone2;
						}
					}
					shipper_res.setPhone(firstNotNull_phone);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							shipper_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							shipper_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return shipper_res;
				}, Encoders.bean(Shipper.class));
			return d;
	}
	
	
	
	
	
	
	public Shipper getShipper(Shipper.ship_via role, Order order) {
		if(role != null) {
			if(role.equals(Shipper.ship_via.shipper))
				return getShipperInShip_viaByOrder(order);
		}
		return null;
	}
	
	public Dataset<Shipper> getShipperList(Shipper.ship_via role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Shipper.ship_via.shipper))
				return getShipperListInShip_viaByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Shipper> getShipperList(Shipper.ship_via role, Condition<ShipperAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Shipper.ship_via.shipper))
				return getShipperListInShip_via(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	public abstract Dataset<Shipper> getShipperListInShip_via(conditions.Condition<conditions.ShipperAttribute> shipper_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public Dataset<Shipper> getShipperListInShip_viaByShipperCondition(conditions.Condition<conditions.ShipperAttribute> shipper_condition){
		return getShipperListInShip_via(shipper_condition, null);
	}
	public Dataset<Shipper> getShipperListInShip_viaByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getShipperListInShip_via(null, order_condition);
	}
	
	public Shipper getShipperInShip_viaByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Shipper> res = getShipperListInShip_viaByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	
	public abstract boolean insertShipper(Shipper shipper);
	
	public abstract boolean insertShipperInShippersFromRelData(Shipper shipper); 
	private boolean inUpdateMethod = false;
	private List<Row> allShipperIdList = null;
	public abstract void updateShipperList(conditions.Condition<conditions.ShipperAttribute> condition, conditions.SetClause<conditions.ShipperAttribute> set);
	
	public void updateShipper(pojo.Shipper shipper) {
		//TODO using the id
		return;
	}
	public abstract void updateShipperListInShip_via(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.ShipperAttribute> set
	);
	
	public void updateShipperListInShip_viaByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.SetClause<conditions.ShipperAttribute> set
	){
		updateShipperListInShip_via(shipper_condition, null, set);
	}
	public void updateShipperListInShip_viaByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.ShipperAttribute> set
	){
		updateShipperListInShip_via(null, order_condition, set);
	}
	
	public void updateShipperInShip_viaByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.ShipperAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteShipperList(conditions.Condition<conditions.ShipperAttribute> condition);
	
	public void deleteShipper(pojo.Shipper shipper) {
		//TODO using the id
		return;
	}
	public abstract void deleteShipperListInShip_via(	
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteShipperListInShip_viaByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition
	){
		deleteShipperListInShip_via(shipper_condition, null);
	}
	public void deleteShipperListInShip_viaByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteShipperListInShip_via(null, order_condition);
	}
	
	public void deleteShipperInShip_viaByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
