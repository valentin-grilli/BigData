package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.ProductInfo;
import conditions.*;
import dao.services.ProductInfoService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Dataset;
import org.apache.spark.sql.Encoders;
import util.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class ProductInfoServiceImpl extends ProductInfoService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductInfoServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromRelData(Condition<ProductInfoAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInProductsInfoFromRelDataWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInProductsInfoFromRelData(conditions.SetClause<ProductInfoAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ProductInfoAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ProductInfoAttribute, Object> e : clause.entrySet()) {
				ProductInfoAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ProductInfoAttribute.id ) {
					res.add("ProductID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.name ) {
					res.add("ProductName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.supplierRef ) {
					res.add("SupplierRef = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.categoryRef ) {
					res.add("CategoryRef = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.quantityPerUnit ) {
					res.add("QuantityPerUnit = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.unitPrice ) {
					res.add("UnitPrice = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.reorderLevel ) {
					res.add("ReorderLevel = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductInfoAttribute.discontinued ) {
					res.add("Discontinued = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromRelDataWithTableAlias(Condition<ProductInfoAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductInfoAttribute attr = ((SimpleCondition<ProductInfoAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductInfoAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductInfoAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductInfoAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "ProductID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.name ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "ProductName " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.supplierRef ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "SupplierRef " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.categoryRef ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "CategoryRef " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.quantityPerUnit ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "QuantityPerUnit " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.unitPrice ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "UnitPrice " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.reorderLevel ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "ReorderLevel " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductInfoAttribute.discontinued ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Discontinued " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == ProductInfoAttribute.id ) {
						if(op == Operator.EQUALS)
							where =  "ProductID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductID IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.name ) {
						if(op == Operator.EQUALS)
							where =  "ProductName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductName IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.supplierRef ) {
						if(op == Operator.EQUALS)
							where =  "SupplierRef IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "SupplierRef IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.categoryRef ) {
						if(op == Operator.EQUALS)
							where =  "CategoryRef IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "CategoryRef IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.quantityPerUnit ) {
						if(op == Operator.EQUALS)
							where =  "QuantityPerUnit IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "QuantityPerUnit IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.unitPrice ) {
						if(op == Operator.EQUALS)
							where =  "UnitPrice IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "UnitPrice IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.reorderLevel ) {
						if(op == Operator.EQUALS)
							where =  "ReorderLevel IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ReorderLevel IS NOT NULL";
					}
					if(attr == ProductInfoAttribute.discontinued ) {
						if(op == Operator.EQUALS)
							where =  "Discontinued IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Discontinued IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductsInfoFromRelData(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductsInfoFromRelData(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductsInfoFromRelData(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductsInfoFromRelData(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	
	public Dataset<ProductInfo> getProductInfoListInProductsInfoFromRelData(conditions.Condition<conditions.ProductInfoAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductInfoServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "ProductsInfo", where);
		
	
		Dataset<ProductInfo> res = d.map((MapFunction<Row, ProductInfo>) r -> {
					ProductInfo productInfo_res = new ProductInfo();
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
	
	
	
					return productInfo_res;
				}, Encoders.bean(ProductInfo.class));
	
	
		return res;
		
	}
	
	
	
	
	//TODO redis
	public Dataset<ProductInfo> getProductInfoListInStockInfoPairsFromRedisDB(conditions.Condition<conditions.ProductInfoAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductInfoAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductInfoAttribute.id));
			keyAttributes.add(ProductInfoAttribute.id);
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
			Set<ProductInfoAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductInfoAttribute a : conditionAttributes) {
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
		Dataset<ProductInfo> res = rows.map((MapFunction<Row, ProductInfo>) r -> {
					ProductInfo productInfo_res = new ProductInfo();
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
					// attribute [ProductInfo.Id]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("productid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'ProductInfo' mapped physical field 'productid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for ProductInfoid attribute stored in db redisDB. Regex [{}] Value [{}]",regex,value);
						productInfo_res.addLogEvent("Cannot retrieve value for ProductInfo.id attribute stored in db redisDB. Probably due to an ambiguous regex.");
					}
					productInfo_res.setId(id == null ? null : Integer.parseInt(id));
	
						return productInfo_res;
				}, Encoders.bean(ProductInfo.class));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<ProductInfo> getProductListInConcern(conditions.Condition<conditions.StockInfoAttribute> stock_condition,conditions.Condition<conditions.ProductInfoAttribute> product_condition)		{
		MutableBoolean product_refilter = new MutableBoolean(false);
		List<Dataset<ProductInfo>> datasetsPOJO = new ArrayList<Dataset<ProductInfo>>();
		Dataset<StockInfo> all = null;
		boolean all_already_persisted = false;
		MutableBoolean stock_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		stock_refilter = new MutableBoolean(false);
		// For role 'stock' in reference 'concerned'  B->A Scenario
		Dataset<StockInfoTDO> stockInfoTDOconcernedstock = concernService.getStockInfoTDOListStockInConcernedInStockInfoPairsFromKv(stock_condition, stock_refilter);
		Dataset<ProductInfoTDO> productInfoTDOconcernedproduct = concernService.getProductInfoTDOListProductInConcernedInStockInfoPairsFromKv(product_condition, product_refilter);
		if(stock_refilter.booleanValue()) {
			if(all == null)
				all = new StockInfoServiceImpl().getStockInfoList(stock_condition);
			joinCondition = null;
			joinCondition = stockInfoTDOconcernedstock.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				stockInfoTDOconcernedstock = stockInfoTDOconcernedstock.as("A").join(all).select("A.*").as(Encoders.bean(StockInfoTDO.class));
			else
				stockInfoTDOconcernedstock = stockInfoTDOconcernedstock.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(StockInfoTDO.class));
		}
		Dataset<Row> res_concerned = 
			productInfoTDOconcernedproduct.join(stockInfoTDOconcernedstock
				.withColumnRenamed("id", "StockInfo_id")
				.withColumnRenamed("unitsInStock", "StockInfo_unitsInStock")
				.withColumnRenamed("unitsOnOrder", "StockInfo_unitsOnOrder")
				.withColumnRenamed("logEvents", "StockInfo_logEvents"),
				productInfoTDOconcernedproduct.col("kv_stockInfoPairs_concerned_ProductID").equalTo(stockInfoTDOconcernedstock.col("kv_stockInfoPairs_concerned_productid")));
		Dataset<ProductInfo> res_ProductInfo_concerned = res_concerned.select( "id", "name", "supplierRef", "categoryRef", "quantityPerUnit", "unitPrice", "reorderLevel", "discontinued", "logEvents").as(Encoders.bean(ProductInfo.class));
		res_ProductInfo_concerned = res_ProductInfo_concerned.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_ProductInfo_concerned);
		
		Dataset<Concern> res_concern_product;
		Dataset<ProductInfo> res_ProductInfo;
		
		
		//Join datasets or return 
		Dataset<ProductInfo> res = fullOuterJoinsProductInfo(datasetsPOJO);
		if(res == null)
			return null;
	
		if(product_refilter.booleanValue())
			res = res.filter((FilterFunction<ProductInfo>) r -> product_condition == null || product_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertProductInfo(ProductInfo productInfo){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertProductInfoInProductsInfoFromRelData(productInfo) || inserted ;
			inserted = insertProductInfoInStockInfoPairsFromRedisDB(productInfo) || inserted ;
		return inserted;
	}
	
	public boolean insertProductInfoInProductsInfoFromRelData(ProductInfo productInfo)	{
		String idvalue="";
		idvalue+=productInfo.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("ProductID");
		values.add(productInfo.getId());
		columns.add("ProductName");
		values.add(productInfo.getName());
		columns.add("SupplierRef");
		values.add(productInfo.getSupplierRef());
		columns.add("CategoryRef");
		values.add(productInfo.getCategoryRef());
		columns.add("QuantityPerUnit");
		values.add(productInfo.getQuantityPerUnit());
		columns.add("UnitPrice");
		values.add(productInfo.getUnitPrice());
		columns.add("ReorderLevel");
		values.add(productInfo.getReorderLevel());
		columns.add("Discontinued");
		values.add(productInfo.getDiscontinued());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "ProductsInfo", "relData");
			logger.info("Inserted [ProductInfo] entity ID [{}] in [ProductsInfo] in database [RelData]", idvalue);
		}
		else
			logger.warn("[ProductInfo] entity ID [{}] already present in [ProductsInfo] in database [RelData]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertProductInfoInStockInfoPairsFromRedisDB(ProductInfo productInfo)	{
		String idvalue="";
		idvalue+=productInfo.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "PRODUCT:";
			key += productInfo.getId();
			key += ":STOCKINFO";
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_UnitsInStock="UnitsInStock";
			String _value_UnitsInStock="";
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsInStock,_value_UnitsInStock));
			toAdd = false;
			String _fieldname_UnitsOnOrder="UnitsOnOrder";
			String _value_UnitsOnOrder="";
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsOnOrder,_value_UnitsOnOrder));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "redisDB");
	
			logger.info("Inserted [ProductInfo] entity ID [{}] in [StockInfoPairs] in database [RedisDB]", idvalue);
		}
		else
			logger.warn("[ProductInfo] entity ID [{}] already present in [StockInfoPairs] in database [RedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allProductInfoIdList = null;
	public void updateProductInfoList(conditions.Condition<conditions.ProductInfoAttribute> condition, conditions.SetClause<conditions.ProductInfoAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInProductsInfoFromRelData = new MutableBoolean(false);
			getSQLWhereClauseInProductsInfoFromRelData(condition, refilterInProductsInfoFromRelData);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInProductsInfoFromRelData.booleanValue())
				updateProductInfoListInProductsInfoFromRelData(condition, set);
		
			MutableBoolean refilterInStockInfoPairsFromRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInStockInfoPairsFromRedisDB.booleanValue())
				updateProductInfoListInStockInfoPairsFromRedisDB(condition, set);
		
	
			if(!refilterInProductsInfoFromRelData.booleanValue())
				updateProductInfoListInProductsInfoFromRelData(condition, set);
			if(!refilterInStockInfoPairsFromRedisDB.booleanValue())
				updateProductInfoListInStockInfoPairsFromRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateProductInfoListInProductsInfoFromRelData(Condition<ProductInfoAttribute> condition, SetClause<ProductInfoAttribute> set) {
		List<String> setClause = ProductInfoServiceImpl.getSQLSetClauseInProductsInfoFromRelData(set);
		String setSQL = null;
		for(int i = 0; i < setClause.size(); i++) {
			if(i == 0)
				setSQL = setClause.get(i);
			else
				setSQL += ", " + setClause.get(i);
		}
		
		if(setSQL == null)
			return;
		
		MutableBoolean refilter = new MutableBoolean(false);
		Pair<String, List<String>> whereClause = ProductInfoServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE ProductsInfo SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "relData");
		} else {
			if(!inUpdateMethod || allProductInfoIdList == null)
				allProductInfoIdList = this.getProductInfoList(condition).select("id").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allProductInfoIdList) {
				Condition<ProductInfoAttribute> conditionId = null;
				conditionId = Condition.simple(ProductInfoAttribute.id, Operator.EQUALS, row.getAs("id"));
				whereClause = ProductInfoServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(conditionId, refilter);
				String sql = "UPDATE ProductsInfo SET " + setSQL;
				String where = whereClause.getKey();
				List<String> preparedValues = whereClause.getValue();
				for(String preparedValue : preparedValues) {
					where = where.replaceFirst("\\?", preparedValue);
				}
				if(where != null)
					sql += " WHERE " + where;
				updateQueries.add(sql);
			}
		
			DBConnectionMgr.updatesInTable(updateQueries, "relData");
		}
		
	}
	public void updateProductInfoListInStockInfoPairsFromRedisDB(Condition<ProductInfoAttribute> condition, SetClause<ProductInfoAttribute> set) {
		//TODO
	}
	
	
	
	public void updateProductInfo(pojo.ProductInfo productinfo) {
		//TODO using the id
		return;
	}
	public void updateProductListInConcern(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		
		conditions.SetClause<conditions.ProductInfoAttribute> set
	){
		//TODO
	}
	
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
	
	
	public void deleteProductInfoList(conditions.Condition<conditions.ProductInfoAttribute> condition){
		//TODO
	}
	
	public void deleteProductInfo(pojo.ProductInfo productinfo) {
		//TODO using the id
		return;
	}
	public void deleteProductListInConcern(	
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,	
		conditions.Condition<conditions.ProductInfoAttribute> product_condition){
			//TODO
		}
	
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
