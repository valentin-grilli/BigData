package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Product;
import conditions.*;
import dao.services.ProductService;
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


public class ProductServiceImpl extends ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromRelData(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInProductsInfoFromRelDataWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInProductsInfoFromRelData(conditions.SetClause<ProductAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ProductAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ProductAttribute, Object> e : clause.entrySet()) {
				ProductAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ProductAttribute.id ) {
					res.add("ProductID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.name ) {
					res.add("ProductName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.supplierRef ) {
					res.add("SupplierRef = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.categoryRef ) {
					res.add("CategoryRef = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.quantityPerUnit ) {
					res.add("QuantityPerUnit = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.unitPrice ) {
					res.add("UnitPrice = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.reorderLevel ) {
					res.add("ReorderLevel = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.discontinued ) {
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
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromRelDataWithTableAlias(Condition<ProductAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductAttribute.id ) {
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
					if(attr == ProductAttribute.name ) {
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
					if(attr == ProductAttribute.supplierRef ) {
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
					if(attr == ProductAttribute.categoryRef ) {
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
					if(attr == ProductAttribute.quantityPerUnit ) {
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
					if(attr == ProductAttribute.unitPrice ) {
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
					if(attr == ProductAttribute.reorderLevel ) {
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
					if(attr == ProductAttribute.discontinued ) {
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
					if(attr == ProductAttribute.id ) {
						if(op == Operator.EQUALS)
							where =  "ProductID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductID IS NOT NULL";
					}
					if(attr == ProductAttribute.name ) {
						if(op == Operator.EQUALS)
							where =  "ProductName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductName IS NOT NULL";
					}
					if(attr == ProductAttribute.supplierRef ) {
						if(op == Operator.EQUALS)
							where =  "SupplierRef IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "SupplierRef IS NOT NULL";
					}
					if(attr == ProductAttribute.categoryRef ) {
						if(op == Operator.EQUALS)
							where =  "CategoryRef IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "CategoryRef IS NOT NULL";
					}
					if(attr == ProductAttribute.quantityPerUnit ) {
						if(op == Operator.EQUALS)
							where =  "QuantityPerUnit IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "QuantityPerUnit IS NOT NULL";
					}
					if(attr == ProductAttribute.unitPrice ) {
						if(op == Operator.EQUALS)
							where =  "UnitPrice IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "UnitPrice IS NOT NULL";
					}
					if(attr == ProductAttribute.reorderLevel ) {
						if(op == Operator.EQUALS)
							where =  "ReorderLevel IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ReorderLevel IS NOT NULL";
					}
					if(attr == ProductAttribute.discontinued ) {
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
	
	
	
	public Dataset<Product> getProductListInProductsInfoFromRelData(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "ProductsInfo", where);
		
	
		Dataset<Product> res = d.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Product.Id]
					Integer id = Util.getIntegerValue(r.getAs("ProductID"));
					product_res.setId(id);
					
					// attribute [Product.Name]
					String name = Util.getStringValue(r.getAs("ProductName"));
					product_res.setName(name);
					
					// attribute [Product.SupplierRef]
					Integer supplierRef = Util.getIntegerValue(r.getAs("SupplierRef"));
					product_res.setSupplierRef(supplierRef);
					
					// attribute [Product.CategoryRef]
					Integer categoryRef = Util.getIntegerValue(r.getAs("CategoryRef"));
					product_res.setCategoryRef(categoryRef);
					
					// attribute [Product.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					product_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [Product.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					product_res.setUnitPrice(unitPrice);
					
					// attribute [Product.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					product_res.setReorderLevel(reorderLevel);
					
					// attribute [Product.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					product_res.setDiscontinued(discontinued);
	
	
	
					return product_res;
				}, Encoders.bean(Product.class));
	
	
		return res;
		
	}
	
	
	
	
	//TODO redis
	public Dataset<Product> getProductListInStockInfoPairsFromRedisDB(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductAttribute.id));
			keyAttributes.add(ProductAttribute.id);
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
			Set<ProductAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductAttribute a : conditionAttributes) {
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
		Dataset<Product> res = rows.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
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
					// attribute [Product.Id]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("productid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Product' mapped physical field 'productid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Productid attribute stored in db redisDB. Regex [{}] Value [{}]",regex,value);
						product_res.addLogEvent("Cannot retrieve value for Product.id attribute stored in db redisDB. Probably due to an ambiguous regex.");
					}
					product_res.setId(id == null ? null : Integer.parseInt(id));
					// attribute [Product.UnitsInStock]
					Integer unitsInStock = r.getAs("UnitsInStock") == null ? null : Integer.parseInt(r.getAs("UnitsInStock"));
					product_res.setUnitsInStock(unitsInStock);
					// attribute [Product.UnitsOnOrder]
					Integer unitsOnOrder = r.getAs("UnitsOnOrder") == null ? null : Integer.parseInt(r.getAs("UnitsOnOrder"));
					product_res.setUnitsOnOrder(unitsOnOrder);
	
						return product_res;
				}, Encoders.bean(Product.class));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Product> getProductListInInsert(conditions.Condition<conditions.SupplierAttribute> supplier_condition,conditions.Condition<conditions.ProductAttribute> product_condition)		{
		MutableBoolean product_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Supplier> all = null;
		boolean all_already_persisted = false;
		MutableBoolean supplier_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Insert> res_insert_product;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInProductsInfoFromRelData(product_condition, new MutableBoolean(false)));
		lonelyProductList.add(getProductListInStockInfoPairsFromRedisDB(product_condition, new MutableBoolean(false)));
		Dataset<Product> lonelyProduct = fullOuterJoinsProduct(lonelyProductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinsProduct(Arrays.asList(res, lonelyProduct));
		}
		if(product_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> product_condition == null || product_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertProduct(
		Product product,
		Supplier	supplierInsert){
			boolean inserted = false;
			// Insert in standalone structures
			inserted = insertProductInProductsInfoFromRelData(product)|| inserted ;
			inserted = insertProductInStockInfoPairsFromRedisDB(product)|| inserted ;
			// Insert in structures containing double embedded role
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			// Insert in ref structures mapped to opposite role of mandatory role  
			return inserted;
		}
	
	public boolean insertProductInProductsInfoFromRelData(Product product)	{
		String idvalue="";
		idvalue+=product.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("ProductID");
		values.add(product.getId());
		columns.add("ProductName");
		values.add(product.getName());
		columns.add("SupplierRef");
		values.add(product.getSupplierRef());
		columns.add("CategoryRef");
		values.add(product.getCategoryRef());
		columns.add("QuantityPerUnit");
		values.add(product.getQuantityPerUnit());
		columns.add("UnitPrice");
		values.add(product.getUnitPrice());
		columns.add("ReorderLevel");
		values.add(product.getReorderLevel());
		columns.add("Discontinued");
		values.add(product.getDiscontinued());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "ProductsInfo", "relData");
			logger.info("Inserted [Product] entity ID [{}] in [ProductsInfo] in database [RelData]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [ProductsInfo] in database [RelData]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertProductInStockInfoPairsFromRedisDB(Product product)	{
		String idvalue="";
		idvalue+=product.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "PRODUCT:";
			key += product.getId();
			key += ":STOCKINFO";
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_UnitsInStock="UnitsInStock";
			String _value_UnitsInStock="";
			if(product.getUnitsInStock()!=null){
				toAdd = true;
				_value_UnitsInStock += product.getUnitsInStock();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsInStock,_value_UnitsInStock));
			toAdd = false;
			String _fieldname_UnitsOnOrder="UnitsOnOrder";
			String _value_UnitsOnOrder="";
			if(product.getUnitsOnOrder()!=null){
				toAdd = true;
				_value_UnitsOnOrder += product.getUnitsOnOrder();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsOnOrder,_value_UnitsOnOrder));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "redisDB");
	
			logger.info("Inserted [Product] entity ID [{}] in [StockInfoPairs] in database [RedisDB]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [StockInfoPairs] in database [RedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allProductIdList = null;
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInProductsInfoFromRelData = new MutableBoolean(false);
			getSQLWhereClauseInProductsInfoFromRelData(condition, refilterInProductsInfoFromRelData);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInProductsInfoFromRelData.booleanValue())
				updateProductListInProductsInfoFromRelData(condition, set);
		
			MutableBoolean refilterInStockInfoPairsFromRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInStockInfoPairsFromRedisDB.booleanValue())
				updateProductListInStockInfoPairsFromRedisDB(condition, set);
		
	
			if(!refilterInProductsInfoFromRelData.booleanValue())
				updateProductListInProductsInfoFromRelData(condition, set);
			if(!refilterInStockInfoPairsFromRedisDB.booleanValue())
				updateProductListInStockInfoPairsFromRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateProductListInProductsInfoFromRelData(Condition<ProductAttribute> condition, SetClause<ProductAttribute> set) {
		List<String> setClause = ProductServiceImpl.getSQLSetClauseInProductsInfoFromRelData(set);
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
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilter);
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
			if(!inUpdateMethod || allProductIdList == null)
				allProductIdList = this.getProductList(condition).select("id").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allProductIdList) {
				Condition<ProductAttribute> conditionId = null;
				conditionId = Condition.simple(ProductAttribute.id, Operator.EQUALS, row.getAs("id"));
				whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(conditionId, refilter);
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
	public void updateProductListInStockInfoPairsFromRedisDB(Condition<ProductAttribute> condition, SetClause<ProductAttribute> set) {
		//TODO
	}
	
	
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void updateProductListInInsert(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
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
	
	
	public void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition){
		//TODO
	}
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void deleteProductListInInsert(	
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,	
		conditions.Condition<conditions.ProductAttribute> product_condition){
			//TODO
		}
	
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
