package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.StockInfo;
import conditions.StockInfoAttribute;
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
import conditions.StockInfoAttribute;
import pojo.Concern;
import conditions.ProductInfoAttribute;
import pojo.ProductInfo;

public abstract class StockInfoService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StockInfoService.class);
	protected ConcernService concernService = new dao.impl.ConcernServiceImpl();
	


	public static enum ROLE_NAME {
		CONCERN_STOCK
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.CONCERN_STOCK, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public StockInfoService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public StockInfoService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<StockInfo> getStockInfoList(){
		return getStockInfoList(null);
	}
	
	public Dataset<StockInfo> getStockInfoList(conditions.Condition<conditions.StockInfoAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<StockInfo>> datasets = new ArrayList<Dataset<StockInfo>>();
		Dataset<StockInfo> d = null;
		d = getStockInfoListInStockInfoPairsFromRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsStockInfo(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<StockInfo>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<StockInfo> getStockInfoListInStockInfoPairsFromRedisDB(conditions.Condition<conditions.StockInfoAttribute> condition, MutableBoolean refilterFlag);
	
	
	public StockInfo getStockInfoById(Integer id){
		Condition cond;
		cond = Condition.simple(StockInfoAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<StockInfo> res = getStockInfoList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<StockInfo> getStockInfoListById(Integer id) {
		return getStockInfoList(conditions.Condition.simple(conditions.StockInfoAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<StockInfo> getStockInfoListByUnitsInStock(Integer unitsInStock) {
		return getStockInfoList(conditions.Condition.simple(conditions.StockInfoAttribute.unitsInStock, conditions.Operator.EQUALS, unitsInStock));
	}
	
	public Dataset<StockInfo> getStockInfoListByUnitsOnOrder(Integer unitsOnOrder) {
		return getStockInfoList(conditions.Condition.simple(conditions.StockInfoAttribute.unitsOnOrder, conditions.Operator.EQUALS, unitsOnOrder));
	}
	
	
	
	public static Dataset<StockInfo> fullOuterJoinsStockInfo(List<Dataset<StockInfo>> datasetsPOJO) {
		return fullOuterJoinsStockInfo(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<StockInfo> fullLeftOuterJoinsStockInfo(List<Dataset<StockInfo>> datasetsPOJO) {
		return fullOuterJoinsStockInfo(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<StockInfo> fullOuterJoinsStockInfo(List<Dataset<StockInfo>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<StockInfo> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [StockInfo] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("unitsInStock", "unitsInStock_1")
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("unitsInStock", "unitsInStock_" + i)
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [StockInfo] objects"); 
			d = res.map((MapFunction<Row, StockInfo>) r -> {
					StockInfo stockInfo_res = new StockInfo();
					
					// attribute 'StockInfo.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					stockInfo_res.setId(firstNotNull_id);
					
					// attribute 'StockInfo.unitsInStock'
					Integer firstNotNull_unitsInStock = Util.getIntegerValue(r.getAs("unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsInStock2 = Util.getIntegerValue(r.getAs("unitsInStock_" + i));
						if (firstNotNull_unitsInStock != null && unitsInStock2 != null && !firstNotNull_unitsInStock.equals(unitsInStock2)) {
							stockInfo_res.addLogEvent("Data consistency problem for [StockInfo - id :"+stockInfo_res.getId()+"]: different values found for attribute 'StockInfo.unitsInStock': " + firstNotNull_unitsInStock + " and " + unitsInStock2 + "." );
							logger.warn("Data consistency problem for [StockInfo - id :"+stockInfo_res.getId()+"]: different values found for attribute 'StockInfo.unitsInStock': " + firstNotNull_unitsInStock + " and " + unitsInStock2 + "." );
						}
						if (firstNotNull_unitsInStock == null && unitsInStock2 != null) {
							firstNotNull_unitsInStock = unitsInStock2;
						}
					}
					stockInfo_res.setUnitsInStock(firstNotNull_unitsInStock);
					
					// attribute 'StockInfo.unitsOnOrder'
					Integer firstNotNull_unitsOnOrder = Util.getIntegerValue(r.getAs("unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsOnOrder2 = Util.getIntegerValue(r.getAs("unitsOnOrder_" + i));
						if (firstNotNull_unitsOnOrder != null && unitsOnOrder2 != null && !firstNotNull_unitsOnOrder.equals(unitsOnOrder2)) {
							stockInfo_res.addLogEvent("Data consistency problem for [StockInfo - id :"+stockInfo_res.getId()+"]: different values found for attribute 'StockInfo.unitsOnOrder': " + firstNotNull_unitsOnOrder + " and " + unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [StockInfo - id :"+stockInfo_res.getId()+"]: different values found for attribute 'StockInfo.unitsOnOrder': " + firstNotNull_unitsOnOrder + " and " + unitsOnOrder2 + "." );
						}
						if (firstNotNull_unitsOnOrder == null && unitsOnOrder2 != null) {
							firstNotNull_unitsOnOrder = unitsOnOrder2;
						}
					}
					stockInfo_res.setUnitsOnOrder(firstNotNull_unitsOnOrder);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							stockInfo_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							stockInfo_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return stockInfo_res;
				}, Encoders.bean(StockInfo.class));
			return d;
	}
	
	
	
	public StockInfo getStockInfo(StockInfo.concern role, ProductInfo productInfo) {
		if(role != null) {
			if(role.equals(StockInfo.concern.stock))
				return getStockInConcernByProduct(productInfo);
		}
		return null;
	}
	
	public Dataset<StockInfo> getStockInfoList(StockInfo.concern role, Condition<ProductInfoAttribute> condition) {
		if(role != null) {
			if(role.equals(StockInfo.concern.stock))
				return getStockListInConcernByProductCondition(condition);
		}
		return null;
	}
	
	public Dataset<StockInfo> getStockInfoList(StockInfo.concern role, Condition<StockInfoAttribute> condition1, Condition<ProductInfoAttribute> condition2) {
		if(role != null) {
			if(role.equals(StockInfo.concern.stock))
				return getStockListInConcern(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<StockInfo> getStockListInConcern(conditions.Condition<conditions.StockInfoAttribute> stock_condition,conditions.Condition<conditions.ProductInfoAttribute> product_condition);
	
	public Dataset<StockInfo> getStockListInConcernByStockCondition(conditions.Condition<conditions.StockInfoAttribute> stock_condition){
		return getStockListInConcern(stock_condition, null);
	}
	public Dataset<StockInfo> getStockListInConcernByProductCondition(conditions.Condition<conditions.ProductInfoAttribute> product_condition){
		return getStockListInConcern(null, product_condition);
	}
	
	public StockInfo getStockInConcernByProduct(pojo.ProductInfo product){
		if(product == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductInfoAttribute.id,Operator.EQUALS, product.getId());
		Dataset<StockInfo> res = getStockListInConcernByProductCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	
	public abstract boolean insertStockInfo(StockInfo stockInfo);
	
	public abstract boolean insertStockInfoInStockInfoPairsFromRedisDB(StockInfo stockInfo); 
	private boolean inUpdateMethod = false;
	private List<Row> allStockInfoIdList = null;
	public abstract void updateStockInfoList(conditions.Condition<conditions.StockInfoAttribute> condition, conditions.SetClause<conditions.StockInfoAttribute> set);
	
	public void updateStockInfo(pojo.StockInfo stockinfo) {
		//TODO using the id
		return;
	}
	public abstract void updateStockListInConcern(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		
		conditions.SetClause<conditions.StockInfoAttribute> set
	);
	
	public void updateStockListInConcernByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.SetClause<conditions.StockInfoAttribute> set
	){
		updateStockListInConcern(stock_condition, null, set);
	}
	public void updateStockListInConcernByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		conditions.SetClause<conditions.StockInfoAttribute> set
	){
		updateStockListInConcern(null, product_condition, set);
	}
	
	public void updateStockInConcernByProduct(
		pojo.ProductInfo product,
		conditions.SetClause<conditions.StockInfoAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteStockInfoList(conditions.Condition<conditions.StockInfoAttribute> condition);
	
	public void deleteStockInfo(pojo.StockInfo stockinfo) {
		//TODO using the id
		return;
	}
	public abstract void deleteStockListInConcern(	
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,	
		conditions.Condition<conditions.ProductInfoAttribute> product_condition);
	
	public void deleteStockListInConcernByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition
	){
		deleteStockListInConcern(stock_condition, null);
	}
	public void deleteStockListInConcernByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition
	){
		deleteStockListInConcern(null, product_condition);
	}
	
	public void deleteStockInConcernByProduct(
		pojo.ProductInfo product 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
