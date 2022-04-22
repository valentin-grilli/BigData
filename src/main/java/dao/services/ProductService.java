package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Product;
import conditions.ProductAttribute;
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
import conditions.ProductAttribute;
import pojo.Insert;
import conditions.SupplierAttribute;
import pojo.Supplier;

public abstract class ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductService.class);
	protected InsertService insertService = new dao.impl.InsertServiceImpl();
	


	public static enum ROLE_NAME {
		INSERT_PRODUCT
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.INSERT_PRODUCT, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ProductService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ProductService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Product> getProductList(){
		return getProductList(null);
	}
	
	public Dataset<Product> getProductList(conditions.Condition<conditions.ProductAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Product>> datasets = new ArrayList<Dataset<Product>>();
		Dataset<Product> d = null;
		d = getProductListInProductsInfoFromRelData(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductListInStockInfoPairsFromRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsProduct(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Product> getProductListInProductsInfoFromRelData(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	public abstract Dataset<Product> getProductListInStockInfoPairsFromRedisDB(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Product getProductById(Integer id){
		Condition cond;
		cond = Condition.simple(ProductAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Product> res = getProductList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Product> getProductListById(Integer id) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Product> getProductListByName(String name) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.name, conditions.Operator.EQUALS, name));
	}
	
	public Dataset<Product> getProductListBySupplierRef(Integer supplierRef) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.supplierRef, conditions.Operator.EQUALS, supplierRef));
	}
	
	public Dataset<Product> getProductListByCategoryRef(Integer categoryRef) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.categoryRef, conditions.Operator.EQUALS, categoryRef));
	}
	
	public Dataset<Product> getProductListByQuantityPerUnit(String quantityPerUnit) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.quantityPerUnit, conditions.Operator.EQUALS, quantityPerUnit));
	}
	
	public Dataset<Product> getProductListByUnitPrice(Double unitPrice) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.unitPrice, conditions.Operator.EQUALS, unitPrice));
	}
	
	public Dataset<Product> getProductListByReorderLevel(Integer reorderLevel) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.reorderLevel, conditions.Operator.EQUALS, reorderLevel));
	}
	
	public Dataset<Product> getProductListByDiscontinued(Boolean discontinued) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.discontinued, conditions.Operator.EQUALS, discontinued));
	}
	
	public Dataset<Product> getProductListByUnitsInStock(Integer unitsInStock) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.unitsInStock, conditions.Operator.EQUALS, unitsInStock));
	}
	
	public Dataset<Product> getProductListByUnitsOnOrder(Integer unitsOnOrder) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.unitsOnOrder, conditions.Operator.EQUALS, unitsOnOrder));
	}
	
	
	
	public static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Product> fullLeftOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Product> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Product] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("name", "name_1")
								.withColumnRenamed("supplierRef", "supplierRef_1")
								.withColumnRenamed("categoryRef", "categoryRef_1")
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_1")
								.withColumnRenamed("unitPrice", "unitPrice_1")
								.withColumnRenamed("reorderLevel", "reorderLevel_1")
								.withColumnRenamed("discontinued", "discontinued_1")
								.withColumnRenamed("unitsInStock", "unitsInStock_1")
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_1")
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
								.withColumnRenamed("unitsInStock", "unitsInStock_" + i)
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Product] objects"); 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					product_res.setId(firstNotNull_id);
					
					// attribute 'Product.name'
					String firstNotNull_name = Util.getStringValue(r.getAs("name"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String name2 = Util.getStringValue(r.getAs("name_" + i));
						if (firstNotNull_name != null && name2 != null && !firstNotNull_name.equals(name2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_name + " and " + name2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_name + " and " + name2 + "." );
						}
						if (firstNotNull_name == null && name2 != null) {
							firstNotNull_name = name2;
						}
					}
					product_res.setName(firstNotNull_name);
					
					// attribute 'Product.supplierRef'
					Integer firstNotNull_supplierRef = Util.getIntegerValue(r.getAs("supplierRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer supplierRef2 = Util.getIntegerValue(r.getAs("supplierRef_" + i));
						if (firstNotNull_supplierRef != null && supplierRef2 != null && !firstNotNull_supplierRef.equals(supplierRef2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_supplierRef + " and " + supplierRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_supplierRef + " and " + supplierRef2 + "." );
						}
						if (firstNotNull_supplierRef == null && supplierRef2 != null) {
							firstNotNull_supplierRef = supplierRef2;
						}
					}
					product_res.setSupplierRef(firstNotNull_supplierRef);
					
					// attribute 'Product.categoryRef'
					Integer firstNotNull_categoryRef = Util.getIntegerValue(r.getAs("categoryRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer categoryRef2 = Util.getIntegerValue(r.getAs("categoryRef_" + i));
						if (firstNotNull_categoryRef != null && categoryRef2 != null && !firstNotNull_categoryRef.equals(categoryRef2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_categoryRef + " and " + categoryRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_categoryRef + " and " + categoryRef2 + "." );
						}
						if (firstNotNull_categoryRef == null && categoryRef2 != null) {
							firstNotNull_categoryRef = categoryRef2;
						}
					}
					product_res.setCategoryRef(firstNotNull_categoryRef);
					
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_quantityPerUnit = Util.getStringValue(r.getAs("quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String quantityPerUnit2 = Util.getStringValue(r.getAs("quantityPerUnit_" + i));
						if (firstNotNull_quantityPerUnit != null && quantityPerUnit2 != null && !firstNotNull_quantityPerUnit.equals(quantityPerUnit2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_quantityPerUnit + " and " + quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_quantityPerUnit + " and " + quantityPerUnit2 + "." );
						}
						if (firstNotNull_quantityPerUnit == null && quantityPerUnit2 != null) {
							firstNotNull_quantityPerUnit = quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_quantityPerUnit);
					
					// attribute 'Product.unitPrice'
					Double firstNotNull_unitPrice = Util.getDoubleValue(r.getAs("unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_unitPrice != null && unitPrice2 != null && !firstNotNull_unitPrice.equals(unitPrice2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_unitPrice == null && unitPrice2 != null) {
							firstNotNull_unitPrice = unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_unitPrice);
					
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_reorderLevel = Util.getIntegerValue(r.getAs("reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer reorderLevel2 = Util.getIntegerValue(r.getAs("reorderLevel_" + i));
						if (firstNotNull_reorderLevel != null && reorderLevel2 != null && !firstNotNull_reorderLevel.equals(reorderLevel2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_reorderLevel + " and " + reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_reorderLevel + " and " + reorderLevel2 + "." );
						}
						if (firstNotNull_reorderLevel == null && reorderLevel2 != null) {
							firstNotNull_reorderLevel = reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_reorderLevel);
					
					// attribute 'Product.discontinued'
					Boolean firstNotNull_discontinued = Util.getBooleanValue(r.getAs("discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean discontinued2 = Util.getBooleanValue(r.getAs("discontinued_" + i));
						if (firstNotNull_discontinued != null && discontinued2 != null && !firstNotNull_discontinued.equals(discontinued2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_discontinued + " and " + discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_discontinued + " and " + discontinued2 + "." );
						}
						if (firstNotNull_discontinued == null && discontinued2 != null) {
							firstNotNull_discontinued = discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_discontinued);
					
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_unitsInStock = Util.getIntegerValue(r.getAs("unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsInStock2 = Util.getIntegerValue(r.getAs("unitsInStock_" + i));
						if (firstNotNull_unitsInStock != null && unitsInStock2 != null && !firstNotNull_unitsInStock.equals(unitsInStock2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_unitsInStock + " and " + unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_unitsInStock + " and " + unitsInStock2 + "." );
						}
						if (firstNotNull_unitsInStock == null && unitsInStock2 != null) {
							firstNotNull_unitsInStock = unitsInStock2;
						}
					}
					product_res.setUnitsInStock(firstNotNull_unitsInStock);
					
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_unitsOnOrder = Util.getIntegerValue(r.getAs("unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsOnOrder2 = Util.getIntegerValue(r.getAs("unitsOnOrder_" + i));
						if (firstNotNull_unitsOnOrder != null && unitsOnOrder2 != null && !firstNotNull_unitsOnOrder.equals(unitsOnOrder2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_unitsOnOrder + " and " + unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_unitsOnOrder + " and " + unitsOnOrder2 + "." );
						}
						if (firstNotNull_unitsOnOrder == null && unitsOnOrder2 != null) {
							firstNotNull_unitsOnOrder = unitsOnOrder2;
						}
					}
					product_res.setUnitsOnOrder(firstNotNull_unitsOnOrder);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							product_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							product_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return product_res;
				}, Encoders.bean(Product.class));
			return d;
	}
	
	
	
	
	
	
	
	
	public Dataset<Product> getProductList(Product.insert role, Supplier supplier) {
		if(role != null) {
			if(role.equals(Product.insert.product))
				return getProductListInInsertBySupplier(supplier);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.insert role, Condition<SupplierAttribute> condition) {
		if(role != null) {
			if(role.equals(Product.insert.product))
				return getProductListInInsertBySupplierCondition(condition);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.insert role, Condition<SupplierAttribute> condition1, Condition<ProductAttribute> condition2) {
		if(role != null) {
			if(role.equals(Product.insert.product))
				return getProductListInInsert(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Product> getProductListInInsert(conditions.Condition<conditions.SupplierAttribute> supplier_condition,conditions.Condition<conditions.ProductAttribute> product_condition);
	
	public Dataset<Product> getProductListInInsertBySupplierCondition(conditions.Condition<conditions.SupplierAttribute> supplier_condition){
		return getProductListInInsert(supplier_condition, null);
	}
	
	public Dataset<Product> getProductListInInsertBySupplier(pojo.Supplier supplier){
		if(supplier == null)
			return null;
	
		Condition c;
		c=Condition.simple(SupplierAttribute.id,Operator.EQUALS, supplier.getId());
		Dataset<Product> res = getProductListInInsertBySupplierCondition(c);
		return res;
	}
	
	public Dataset<Product> getProductListInInsertByProductCondition(conditions.Condition<conditions.ProductAttribute> product_condition){
		return getProductListInInsert(null, product_condition);
	}
	
	public abstract boolean insertProduct(
		Product product,
		Supplier	supplierInsert);
	
	public abstract boolean insertProductInProductsInfoFromRelData(Product product); 
	public abstract boolean insertProductInStockInfoPairsFromRedisDB(Product product); 
	private boolean inUpdateMethod = false;
	private List<Row> allProductIdList = null;
	public abstract void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set);
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void updateProductListInInsert(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
	public void updateProductListInInsertBySupplierCondition(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductListInInsert(supplier_condition, null, set);
	}
	
	public void updateProductListInInsertBySupplier(
		pojo.Supplier supplier,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateProductListInInsertByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductListInInsert(null, product_condition, set);
	}
	
	
	public abstract void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition);
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void deleteProductListInInsert(	
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,	
		conditions.Condition<conditions.ProductAttribute> product_condition);
	
	public void deleteProductListInInsertBySupplierCondition(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition
	){
		deleteProductListInInsert(supplier_condition, null);
	}
	
	public void deleteProductListInInsertBySupplier(
		pojo.Supplier supplier 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteProductListInInsertByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteProductListInInsert(null, product_condition);
	}
	
}
