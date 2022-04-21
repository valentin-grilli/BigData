package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.ProductInfo;
import conditions.ProductInfoAttribute;
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
import conditions.ProductInfoAttribute;
import pojo.Concern;
import conditions.StockInfoAttribute;
import pojo.StockInfo;

public abstract class ProductInfoService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductInfoService.class);
	protected ConcernService concernService = new dao.impl.ConcernServiceImpl();
	


	public static enum ROLE_NAME {
		CONCERN_PRODUCT
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.CONCERN_PRODUCT, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ProductInfoService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ProductInfoService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<ProductInfo> getProductInfoList(){
		return getProductInfoList(null);
	}
	
	public Dataset<ProductInfo> getProductInfoList(conditions.Condition<conditions.ProductInfoAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<ProductInfo>> datasets = new ArrayList<Dataset<ProductInfo>>();
		Dataset<ProductInfo> d = null;
		d = getProductInfoListInProductsInfoFromRelData(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductInfoListInStockInfoPairsFromRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsProductInfo(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<ProductInfo>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<ProductInfo> getProductInfoListInProductsInfoFromRelData(conditions.Condition<conditions.ProductInfoAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	public abstract Dataset<ProductInfo> getProductInfoListInStockInfoPairsFromRedisDB(conditions.Condition<conditions.ProductInfoAttribute> condition, MutableBoolean refilterFlag);
	
	
	public ProductInfo getProductInfoById(Integer id){
		Condition cond;
		cond = Condition.simple(ProductInfoAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<ProductInfo> res = getProductInfoList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<ProductInfo> getProductInfoListById(Integer id) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<ProductInfo> getProductInfoListByName(String name) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.name, conditions.Operator.EQUALS, name));
	}
	
	public Dataset<ProductInfo> getProductInfoListBySupplierRef(Integer supplierRef) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.supplierRef, conditions.Operator.EQUALS, supplierRef));
	}
	
	public Dataset<ProductInfo> getProductInfoListByCategoryRef(Integer categoryRef) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.categoryRef, conditions.Operator.EQUALS, categoryRef));
	}
	
	public Dataset<ProductInfo> getProductInfoListByQuantityPerUnit(String quantityPerUnit) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.quantityPerUnit, conditions.Operator.EQUALS, quantityPerUnit));
	}
	
	public Dataset<ProductInfo> getProductInfoListByUnitPrice(Double unitPrice) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.unitPrice, conditions.Operator.EQUALS, unitPrice));
	}
	
	public Dataset<ProductInfo> getProductInfoListByReorderLevel(Integer reorderLevel) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.reorderLevel, conditions.Operator.EQUALS, reorderLevel));
	}
	
	public Dataset<ProductInfo> getProductInfoListByDiscontinued(Boolean discontinued) {
		return getProductInfoList(conditions.Condition.simple(conditions.ProductInfoAttribute.discontinued, conditions.Operator.EQUALS, discontinued));
	}
	
	
	
	public static Dataset<ProductInfo> fullOuterJoinsProductInfo(List<Dataset<ProductInfo>> datasetsPOJO) {
		return fullOuterJoinsProductInfo(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<ProductInfo> fullLeftOuterJoinsProductInfo(List<Dataset<ProductInfo>> datasetsPOJO) {
		return fullOuterJoinsProductInfo(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<ProductInfo> fullOuterJoinsProductInfo(List<Dataset<ProductInfo>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<ProductInfo> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [ProductInfo] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("name", "name_1")
								.withColumnRenamed("supplierRef", "supplierRef_1")
								.withColumnRenamed("categoryRef", "categoryRef_1")
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_1")
								.withColumnRenamed("unitPrice", "unitPrice_1")
								.withColumnRenamed("reorderLevel", "reorderLevel_1")
								.withColumnRenamed("discontinued", "discontinued_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("name", "name_" + i)
								.withColumnRenamed("supplierRef", "supplierRef_" + i)
								.withColumnRenamed("categoryRef", "categoryRef_" + i)
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_" + i)
								.withColumnRenamed("unitPrice", "unitPrice_" + i)
								.withColumnRenamed("reorderLevel", "reorderLevel_" + i)
								.withColumnRenamed("discontinued", "discontinued_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [ProductInfo] objects"); 
			d = res.map((MapFunction<Row, ProductInfo>) r -> {
					ProductInfo productInfo_res = new ProductInfo();
					
					// attribute 'ProductInfo.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					productInfo_res.setId(firstNotNull_id);
					
					// attribute 'ProductInfo.name'
					String firstNotNull_name = Util.getStringValue(r.getAs("name"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String name2 = Util.getStringValue(r.getAs("name_" + i));
						if (firstNotNull_name != null && name2 != null && !firstNotNull_name.equals(name2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.name': " + firstNotNull_name + " and " + name2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.name': " + firstNotNull_name + " and " + name2 + "." );
						}
						if (firstNotNull_name == null && name2 != null) {
							firstNotNull_name = name2;
						}
					}
					productInfo_res.setName(firstNotNull_name);
					
					// attribute 'ProductInfo.supplierRef'
					Integer firstNotNull_supplierRef = Util.getIntegerValue(r.getAs("supplierRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer supplierRef2 = Util.getIntegerValue(r.getAs("supplierRef_" + i));
						if (firstNotNull_supplierRef != null && supplierRef2 != null && !firstNotNull_supplierRef.equals(supplierRef2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.supplierRef': " + firstNotNull_supplierRef + " and " + supplierRef2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.supplierRef': " + firstNotNull_supplierRef + " and " + supplierRef2 + "." );
						}
						if (firstNotNull_supplierRef == null && supplierRef2 != null) {
							firstNotNull_supplierRef = supplierRef2;
						}
					}
					productInfo_res.setSupplierRef(firstNotNull_supplierRef);
					
					// attribute 'ProductInfo.categoryRef'
					Integer firstNotNull_categoryRef = Util.getIntegerValue(r.getAs("categoryRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer categoryRef2 = Util.getIntegerValue(r.getAs("categoryRef_" + i));
						if (firstNotNull_categoryRef != null && categoryRef2 != null && !firstNotNull_categoryRef.equals(categoryRef2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.categoryRef': " + firstNotNull_categoryRef + " and " + categoryRef2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.categoryRef': " + firstNotNull_categoryRef + " and " + categoryRef2 + "." );
						}
						if (firstNotNull_categoryRef == null && categoryRef2 != null) {
							firstNotNull_categoryRef = categoryRef2;
						}
					}
					productInfo_res.setCategoryRef(firstNotNull_categoryRef);
					
					// attribute 'ProductInfo.quantityPerUnit'
					String firstNotNull_quantityPerUnit = Util.getStringValue(r.getAs("quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String quantityPerUnit2 = Util.getStringValue(r.getAs("quantityPerUnit_" + i));
						if (firstNotNull_quantityPerUnit != null && quantityPerUnit2 != null && !firstNotNull_quantityPerUnit.equals(quantityPerUnit2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.quantityPerUnit': " + firstNotNull_quantityPerUnit + " and " + quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.quantityPerUnit': " + firstNotNull_quantityPerUnit + " and " + quantityPerUnit2 + "." );
						}
						if (firstNotNull_quantityPerUnit == null && quantityPerUnit2 != null) {
							firstNotNull_quantityPerUnit = quantityPerUnit2;
						}
					}
					productInfo_res.setQuantityPerUnit(firstNotNull_quantityPerUnit);
					
					// attribute 'ProductInfo.unitPrice'
					Double firstNotNull_unitPrice = Util.getDoubleValue(r.getAs("unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_unitPrice != null && unitPrice2 != null && !firstNotNull_unitPrice.equals(unitPrice2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_unitPrice == null && unitPrice2 != null) {
							firstNotNull_unitPrice = unitPrice2;
						}
					}
					productInfo_res.setUnitPrice(firstNotNull_unitPrice);
					
					// attribute 'ProductInfo.reorderLevel'
					Integer firstNotNull_reorderLevel = Util.getIntegerValue(r.getAs("reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer reorderLevel2 = Util.getIntegerValue(r.getAs("reorderLevel_" + i));
						if (firstNotNull_reorderLevel != null && reorderLevel2 != null && !firstNotNull_reorderLevel.equals(reorderLevel2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.reorderLevel': " + firstNotNull_reorderLevel + " and " + reorderLevel2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.reorderLevel': " + firstNotNull_reorderLevel + " and " + reorderLevel2 + "." );
						}
						if (firstNotNull_reorderLevel == null && reorderLevel2 != null) {
							firstNotNull_reorderLevel = reorderLevel2;
						}
					}
					productInfo_res.setReorderLevel(firstNotNull_reorderLevel);
					
					// attribute 'ProductInfo.discontinued'
					Boolean firstNotNull_discontinued = Util.getBooleanValue(r.getAs("discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean discontinued2 = Util.getBooleanValue(r.getAs("discontinued_" + i));
						if (firstNotNull_discontinued != null && discontinued2 != null && !firstNotNull_discontinued.equals(discontinued2)) {
							productInfo_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.discontinued': " + firstNotNull_discontinued + " and " + discontinued2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+productInfo_res.getId()+"]: different values found for attribute 'ProductInfo.discontinued': " + firstNotNull_discontinued + " and " + discontinued2 + "." );
						}
						if (firstNotNull_discontinued == null && discontinued2 != null) {
							firstNotNull_discontinued = discontinued2;
						}
					}
					productInfo_res.setDiscontinued(firstNotNull_discontinued);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							productInfo_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							productInfo_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return productInfo_res;
				}, Encoders.bean(ProductInfo.class));
			return d;
	}
	
	
	
	public ProductInfo getProductInfo(ProductInfo.concern role, StockInfo stockInfo) {
		if(role != null) {
			if(role.equals(ProductInfo.concern.product))
				return getProductInConcernByStock(stockInfo);
		}
		return null;
	}
	
	public Dataset<ProductInfo> getProductInfoList(ProductInfo.concern role, Condition<StockInfoAttribute> condition) {
		if(role != null) {
			if(role.equals(ProductInfo.concern.product))
				return getProductListInConcernByStockCondition(condition);
		}
		return null;
	}
	
	public Dataset<ProductInfo> getProductInfoList(ProductInfo.concern role, Condition<StockInfoAttribute> condition1, Condition<ProductInfoAttribute> condition2) {
		if(role != null) {
			if(role.equals(ProductInfo.concern.product))
				return getProductListInConcern(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<ProductInfo> getProductListInConcern(conditions.Condition<conditions.StockInfoAttribute> stock_condition,conditions.Condition<conditions.ProductInfoAttribute> product_condition);
	
	public Dataset<ProductInfo> getProductListInConcernByStockCondition(conditions.Condition<conditions.StockInfoAttribute> stock_condition){
		return getProductListInConcern(stock_condition, null);
	}
	
	public ProductInfo getProductInConcernByStock(pojo.StockInfo stock){
		if(stock == null)
			return null;
	
		Condition c;
		c=Condition.simple(StockInfoAttribute.id,Operator.EQUALS, stock.getId());
		Dataset<ProductInfo> res = getProductListInConcernByStockCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<ProductInfo> getProductListInConcernByProductCondition(conditions.Condition<conditions.ProductInfoAttribute> product_condition){
		return getProductListInConcern(null, product_condition);
	}
	
	
	public abstract boolean insertProductInfo(ProductInfo productInfo);
	
	public abstract boolean insertProductInfoInProductsInfoFromRelData(ProductInfo productInfo); 
	public abstract boolean insertProductInfoInStockInfoPairsFromRedisDB(ProductInfo productInfo); 
	private boolean inUpdateMethod = false;
	private List<Row> allProductInfoIdList = null;
	public abstract void updateProductInfoList(conditions.Condition<conditions.ProductInfoAttribute> condition, conditions.SetClause<conditions.ProductInfoAttribute> set);
	
	public void updateProductInfo(pojo.ProductInfo productinfo) {
		//TODO using the id
		return;
	}
	public abstract void updateProductListInConcern(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		
		conditions.SetClause<conditions.ProductInfoAttribute> set
	);
	
	public void updateProductListInConcernByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.SetClause<conditions.ProductInfoAttribute> set
	){
		updateProductListInConcern(stock_condition, null, set);
	}
	
	public void updateProductInConcernByStock(
		pojo.StockInfo stock,
		conditions.SetClause<conditions.ProductInfoAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateProductListInConcernByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		conditions.SetClause<conditions.ProductInfoAttribute> set
	){
		updateProductListInConcern(null, product_condition, set);
	}
	
	
	public abstract void deleteProductInfoList(conditions.Condition<conditions.ProductInfoAttribute> condition);
	
	public void deleteProductInfo(pojo.ProductInfo productinfo) {
		//TODO using the id
		return;
	}
	public abstract void deleteProductListInConcern(	
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,	
		conditions.Condition<conditions.ProductInfoAttribute> product_condition);
	
	public void deleteProductListInConcernByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition
	){
		deleteProductListInConcern(stock_condition, null);
	}
	
	public void deleteProductInConcernByStock(
		pojo.StockInfo stock 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteProductListInConcernByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition
	){
		deleteProductListInConcern(null, product_condition);
	}
	
}
