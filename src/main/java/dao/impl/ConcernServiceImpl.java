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
import conditions.ConcernAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.StockInfoTDO;
import tdo.ConcernTDO;
import conditions.StockInfoAttribute;
import dao.services.StockInfoService;
import tdo.ProductInfoTDO;
import tdo.ConcernTDO;
import conditions.ProductInfoAttribute;
import dao.services.ProductInfoService;
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

public class ConcernServiceImpl extends dao.services.ConcernService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConcernServiceImpl.class);
	
	
	// Left side 'productid' of reference [concerned ]
	public Dataset<StockInfoTDO> getStockInfoTDOListStockInConcernedInStockInfoPairsFromKv(Condition<StockInfoAttribute> condition, MutableBoolean refilterFlag){	
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<StockInfoAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,StockInfoAttribute.id));
			keyAttributes.add(StockInfoAttribute.id);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("productid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":STOCKINFO");
		keypatternAllVariables=keypatternAllVariables.concat(":STOCKINFO");
		if(!refilterFlag.booleanValue()){
			Set<StockInfoAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (StockInfoAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
	
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		StructType structType = new StructType(new StructField[] {
			DataTypes.createStructField("_id", DataTypes.StringType, true), //technical field to store the key.
			DataTypes.createStructField("UnitsInStock", DataTypes.StringType, true)
	,		DataTypes.createStructField("UnitsOnOrder", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("redisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<StockInfoTDO> res = rows.map((MapFunction<Row, StockInfoTDO>) r -> {
					StockInfoTDO stockInfo_res = new StockInfoTDO();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					String key = isStriped ? prefix + r.getAs("_id") : r.getAs("_id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					// attribute [StockInfo.Id]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("productid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'StockInfo' mapped physical field 'productid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for StockInfoid attribute stored in db redisDB. Regex [{}] Value [{}]",regex,value);
						stockInfo_res.addLogEvent("Cannot retrieve value for StockInfo.id attribute stored in db redisDB. Probably due to an ambiguous regex.");
					}
					stockInfo_res.setId(id == null ? null : Integer.parseInt(id));
					// attribute [StockInfo.UnitsInStock]
					Integer unitsInStock = r.getAs("UnitsInStock") == null ? null : Integer.parseInt(r.getAs("UnitsInStock"));
					stockInfo_res.setUnitsInStock(unitsInStock);
					// attribute [StockInfo.UnitsOnOrder]
					Integer unitsOnOrder = r.getAs("UnitsOnOrder") == null ? null : Integer.parseInt(r.getAs("UnitsOnOrder"));
					stockInfo_res.setUnitsOnOrder(unitsOnOrder);
					//Checking that reference field 'productid' is mapped in Key
					if(fieldsListInKey.contains("productid")){
						//Retrieving reference field 'productid' in Key
						Pattern pattern_productid = Pattern.compile("\\*");
				        Matcher match_productid = pattern_productid.matcher(finalKeypattern);
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("productid")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'StockInfo' mapped physical field 'productid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String concerned_productid = null;
						if(matches) {
						concerned_productid = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'productid'. Regex [{}] Value [{}]",regex,value);
						stockInfo_res.addLogEvent("Cannot retrieve value for 'productid' attribute stored in db redisDB. Probably due to an ambiguous regex.");
						}
						stockInfo_res.setKv_stockInfoPairs_concerned_productid(concerned_productid);
					}
	
						return stockInfo_res;
				}, Encoders.bean(StockInfoTDO.class));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
	}
	
	// Right side 'ProductID' of reference [concerned ]
	public Dataset<ProductInfoTDO> getProductInfoTDOListProductInConcernedInStockInfoPairsFromKv(Condition<ProductInfoAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductInfoServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "ProductsInfo", where);
		
	
		Dataset<ProductInfoTDO> res = d.map((MapFunction<Row, ProductInfoTDO>) r -> {
					ProductInfoTDO productInfo_res = new ProductInfoTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [ProductInfo.Id]
					Integer id = Util.getIntegerValue(r.getAs("ProductID"));
					productInfo_res.setId(id);
					
					// attribute [ProductInfo.Name]
					String name = Util.getStringValue(r.getAs("ProductName"));
					productInfo_res.setName(name);
					
					// attribute [ProductInfo.SupplierRef]
					Integer supplierRef = Util.getIntegerValue(r.getAs("SupplierRef"));
					productInfo_res.setSupplierRef(supplierRef);
					
					// attribute [ProductInfo.CategoryRef]
					Integer categoryRef = Util.getIntegerValue(r.getAs("CategoryRef"));
					productInfo_res.setCategoryRef(categoryRef);
					
					// attribute [ProductInfo.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					productInfo_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [ProductInfo.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					productInfo_res.setUnitPrice(unitPrice);
					
					// attribute [ProductInfo.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					productInfo_res.setReorderLevel(reorderLevel);
					
					// attribute [ProductInfo.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					productInfo_res.setDiscontinued(discontinued);
	
					// Get reference column [ProductID ] for reference [concerned]
					String kv_stockInfoPairs_concerned_ProductID = r.getAs("ProductID") == null ? null : r.getAs("ProductID").toString();
					productInfo_res.setKv_stockInfoPairs_concerned_ProductID(kv_stockInfoPairs_concerned_ProductID);
	
	
					return productInfo_res;
				}, Encoders.bean(ProductInfoTDO.class));
	
	
		return res;}
	
	
	
	
	public Dataset<Concern> getConcernList(
		Condition<StockInfoAttribute> stock_condition,
		Condition<ProductInfoAttribute> product_condition){
			ConcernServiceImpl concernService = this;
			StockInfoService stockInfoService = new StockInfoServiceImpl();  
			ProductInfoService productInfoService = new ProductInfoServiceImpl();
			MutableBoolean stock_refilter = new MutableBoolean(false);
			List<Dataset<Concern>> datasetsPOJO = new ArrayList<Dataset<Concern>>();
			boolean all_already_persisted = false;
			MutableBoolean product_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'stock' in reference 'concerned'. A->B Scenario
			product_refilter = new MutableBoolean(false);
			Dataset<StockInfoTDO> stockInfoTDOconcernedstock = concernService.getStockInfoTDOListStockInConcernedInStockInfoPairsFromKv(stock_condition, stock_refilter);
			Dataset<ProductInfoTDO> productInfoTDOconcernedproduct = concernService.getProductInfoTDOListProductInConcernedInStockInfoPairsFromKv(product_condition, product_refilter);
			
			Dataset<Row> res_concerned_temp = stockInfoTDOconcernedstock.join(productInfoTDOconcernedproduct
					.withColumnRenamed("id", "ProductInfo_id")
					.withColumnRenamed("name", "ProductInfo_name")
					.withColumnRenamed("supplierRef", "ProductInfo_supplierRef")
					.withColumnRenamed("categoryRef", "ProductInfo_categoryRef")
					.withColumnRenamed("quantityPerUnit", "ProductInfo_quantityPerUnit")
					.withColumnRenamed("unitPrice", "ProductInfo_unitPrice")
					.withColumnRenamed("reorderLevel", "ProductInfo_reorderLevel")
					.withColumnRenamed("discontinued", "ProductInfo_discontinued")
					.withColumnRenamed("logEvents", "ProductInfo_logEvents"),
					stockInfoTDOconcernedstock.col("kv_stockInfoPairs_concerned_productid").equalTo(productInfoTDOconcernedproduct.col("kv_stockInfoPairs_concerned_ProductID")));
		
			Dataset<Concern> res_concerned = res_concerned_temp.map(
				(MapFunction<Row, Concern>) r -> {
					Concern res = new Concern();
					StockInfo A = new StockInfo();
					ProductInfo B = new ProductInfo();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setUnitsInStock(Util.getIntegerValue(r.getAs("unitsInStock")));
					A.setUnitsOnOrder(Util.getIntegerValue(r.getAs("unitsOnOrder")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("ProductInfo_id")));
					B.setName(Util.getStringValue(r.getAs("ProductInfo_name")));
					B.setSupplierRef(Util.getIntegerValue(r.getAs("ProductInfo_supplierRef")));
					B.setCategoryRef(Util.getIntegerValue(r.getAs("ProductInfo_categoryRef")));
					B.setQuantityPerUnit(Util.getStringValue(r.getAs("ProductInfo_quantityPerUnit")));
					B.setUnitPrice(Util.getDoubleValue(r.getAs("ProductInfo_unitPrice")));
					B.setReorderLevel(Util.getIntegerValue(r.getAs("ProductInfo_reorderLevel")));
					B.setDiscontinued(Util.getBooleanValue(r.getAs("ProductInfo_discontinued")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("ProductInfo_logEvents")));
						
					res.setStock(A);
					res.setProduct(B);
					return res;
				},Encoders.bean(Concern.class)
			);
		
			datasetsPOJO.add(res_concerned);
		
			
			Dataset<Concern> res_concern_stock;
			Dataset<StockInfo> res_StockInfo;
			
			
			//Join datasets or return 
			Dataset<Concern> res = fullOuterJoinsConcern(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<StockInfo> lonelyStock = null;
			Dataset<ProductInfo> lonelyProduct = null;
			
		
		
			
			if(stock_refilter.booleanValue() || product_refilter.booleanValue())
				res = res.filter((FilterFunction<Concern>) r -> (stock_condition == null || stock_condition.evaluate(r.getStock())) && (product_condition == null || product_condition.evaluate(r.getProduct())));
			
		
			return res;
		
		}
	
	public Dataset<Concern> getConcernListByStockCondition(
		Condition<StockInfoAttribute> stock_condition
	){
		return getConcernList(stock_condition, null);
	}
	
	public Concern getConcernByStock(StockInfo stock) {
		Condition<StockInfoAttribute> cond = null;
		cond = Condition.simple(StockInfoAttribute.id, Operator.EQUALS, stock.getId());
		Dataset<Concern> res = getConcernListByStockCondition(cond);
		List<Concern> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Concern> getConcernListByProductCondition(
		Condition<ProductInfoAttribute> product_condition
	){
		return getConcernList(null, product_condition);
	}
	
	public Concern getConcernByProduct(ProductInfo product) {
		Condition<ProductInfoAttribute> cond = null;
		cond = Condition.simple(ProductInfoAttribute.id, Operator.EQUALS, product.getId());
		Dataset<Concern> res = getConcernListByProductCondition(cond);
		List<Concern> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	public void insertConcern(Concern concern){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertConcernInRefStructStockInfoPairsInRedisDB(concern);
	}
	
	
	
	public 	boolean insertConcernInRefStructStockInfoPairsInRedisDB(Concern concern){
	 	// Rel 'concern' Insert in reference structure 'stockInfoPairs'
		StockInfo stockInfoStock = concern.getStock();
		ProductInfo productInfoProduct = concern.getProduct();
	
		throw new UnsupportedOperationException("Not Implemented yet");
	
	}
	
	
	
	
	public void deleteConcernList(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.Condition<conditions.ProductInfoAttribute> product_condition){
			//TODO
		}
	
	public void deleteConcernListByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition
	){
		deleteConcernList(stock_condition, null);
	}
	
	public void deleteConcernByStock(pojo.StockInfo stock) {
		// TODO using id for selecting
		return;
	}
	public void deleteConcernListByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition
	){
		deleteConcernList(null, product_condition);
	}
	
	public void deleteConcernByProduct(pojo.ProductInfo product) {
		// TODO using id for selecting
		return;
	}
		
}
