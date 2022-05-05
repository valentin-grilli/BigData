package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Products;
import conditions.*;
import dao.services.ProductsService;
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


public class ProductsServiceImpl extends ProductsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductsServiceImpl.class);
	
	
	
	
	
	
	
	//TODO redis
	public Dataset<Products> getProductsListInProductsStockInfoFromMyRedisDB(conditions.Condition<conditions.ProductsAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductsAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductsAttribute.productId));
			keyAttributes.add(ProductsAttribute.productId);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("ProductID");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":STOCKINFO");
		keypatternAllVariables=keypatternAllVariables.concat(":STOCKINFO");
		if(!refilterFlag.booleanValue()){
			Set<ProductsAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductsAttribute a : conditionAttributes) {
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
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myRedisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<Products> res = rows.map((MapFunction<Row, Products>) r -> {
					Products products_res = new Products();
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
					// attribute [Products.ProductId]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("ProductID")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Products' mapped physical field 'ProductID' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String productId = null;
					if(matches) {
						productId = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for ProductsproductId attribute stored in db myRedisDB. Regex [{}] Value [{}]",regex,value);
						products_res.addLogEvent("Cannot retrieve value for Products.productId attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
					}
					products_res.setProductId(productId == null ? null : Integer.parseInt(productId));
					// attribute [Products.UnitsInStock]
					Integer unitsInStock = r.getAs("UnitsInStock") == null ? null : Integer.parseInt(r.getAs("UnitsInStock"));
					products_res.setUnitsInStock(unitsInStock);
					// attribute [Products.UnitsOnOrder]
					Integer unitsOnOrder = r.getAs("UnitsOnOrder") == null ? null : Integer.parseInt(r.getAs("UnitsOnOrder"));
					products_res.setUnitsOnOrder(unitsOnOrder);
	
						return products_res;
				}, Encoders.bean(Products.class));
		res=res.dropDuplicates(new String[] {"productId"});
		return res;
		
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromMyRelDB(Condition<ProductsAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInProductsInfoFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInProductsInfoFromMyRelDB(conditions.SetClause<ProductsAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ProductsAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ProductsAttribute, Object> e : clause.entrySet()) {
				ProductsAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ProductsAttribute.productId ) {
					res.add("ProductID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductsAttribute.productName ) {
					res.add("ProductName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductsAttribute.quantityPerUnit ) {
					res.add("QuantityPerUnit = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductsAttribute.unitPrice ) {
					res.add("UnitPrice = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductsAttribute.reorderLevel ) {
					res.add("ReorderLevel = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductsAttribute.discontinued ) {
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
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromMyRelDBWithTableAlias(Condition<ProductsAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductsAttribute attr = ((SimpleCondition<ProductsAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductsAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductsAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductsAttribute.productId ) {
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
					if(attr == ProductsAttribute.productName ) {
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
					if(attr == ProductsAttribute.quantityPerUnit ) {
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
					if(attr == ProductsAttribute.unitPrice ) {
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
					if(attr == ProductsAttribute.reorderLevel ) {
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
					if(attr == ProductsAttribute.discontinued ) {
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
					if(attr == ProductsAttribute.productId ) {
						if(op == Operator.EQUALS)
							where =  "ProductID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductID IS NOT NULL";
					}
					if(attr == ProductsAttribute.productName ) {
						if(op == Operator.EQUALS)
							where =  "ProductName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductName IS NOT NULL";
					}
					if(attr == ProductsAttribute.quantityPerUnit ) {
						if(op == Operator.EQUALS)
							where =  "QuantityPerUnit IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "QuantityPerUnit IS NOT NULL";
					}
					if(attr == ProductsAttribute.unitPrice ) {
						if(op == Operator.EQUALS)
							where =  "UnitPrice IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "UnitPrice IS NOT NULL";
					}
					if(attr == ProductsAttribute.reorderLevel ) {
						if(op == Operator.EQUALS)
							where =  "ReorderLevel IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ReorderLevel IS NOT NULL";
					}
					if(attr == ProductsAttribute.discontinued ) {
						if(op == Operator.EQUALS)
							where =  "Discontinued IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Discontinued IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductsInfoFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductsInfoFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductsInfoFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductsInfoFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	public Dataset<Products> getProductsListInProductsInfoFromMyRelDB(conditions.Condition<conditions.ProductsAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsInfoFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "ProductsInfo", where);
		
	
		Dataset<Products> res = d.map((MapFunction<Row, Products>) r -> {
					Products products_res = new Products();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Products.ProductId]
					Integer productId = Util.getIntegerValue(r.getAs("ProductID"));
					products_res.setProductId(productId);
					
					// attribute [Products.ProductName]
					String productName = Util.getStringValue(r.getAs("ProductName"));
					products_res.setProductName(productName);
					
					// attribute [Products.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					products_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [Products.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					products_res.setUnitPrice(unitPrice);
					
					// attribute [Products.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					products_res.setReorderLevel(reorderLevel);
					
					// attribute [Products.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					products_res.setDiscontinued(discontinued);
	
	
	
					return products_res;
				}, Encoders.bean(Products.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Products> getSuppliedProductListInSupply(conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,conditions.Condition<conditions.SuppliersAttribute> supplier_condition)		{
		MutableBoolean suppliedProduct_refilter = new MutableBoolean(false);
		List<Dataset<Products>> datasetsPOJO = new ArrayList<Dataset<Products>>();
		Dataset<Suppliers> all = null;
		boolean all_already_persisted = false;
		MutableBoolean supplier_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'suppliedProduct' in reference 'supply'. A->B Scenario
		supplier_refilter = new MutableBoolean(false);
		Dataset<ProductsTDO> productsTDOsupplysuppliedProduct = supplyService.getProductsTDOListSuppliedProductInSupplyInProductsInfoFromRelDB(suppliedProduct_condition, suppliedProduct_refilter);
		Dataset<SuppliersTDO> suppliersTDOsupplysupplier = supplyService.getSuppliersTDOListSupplierInSupplyInProductsInfoFromRelDB(supplier_condition, supplier_refilter);
		if(supplier_refilter.booleanValue()) {
			if(all == null)
				all = new SuppliersServiceImpl().getSuppliersList(supplier_condition);
			joinCondition = null;
			joinCondition = suppliersTDOsupplysupplier.col("supplierId").equalTo(all.col("supplierId"));
			if(joinCondition == null)
				suppliersTDOsupplysupplier = suppliersTDOsupplysupplier.as("A").join(all).select("A.*").as(Encoders.bean(SuppliersTDO.class));
			else
				suppliersTDOsupplysupplier = suppliersTDOsupplysupplier.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(SuppliersTDO.class));
		}
	
		
		Dataset<Row> res_supply = productsTDOsupplysuppliedProduct.join(suppliersTDOsupplysupplier
				.withColumnRenamed("supplierId", "Suppliers_supplierId")
				.withColumnRenamed("companyName", "Suppliers_companyName")
				.withColumnRenamed("contactName", "Suppliers_contactName")
				.withColumnRenamed("contactTitle", "Suppliers_contactTitle")
				.withColumnRenamed("address", "Suppliers_address")
				.withColumnRenamed("city", "Suppliers_city")
				.withColumnRenamed("region", "Suppliers_region")
				.withColumnRenamed("postalCode", "Suppliers_postalCode")
				.withColumnRenamed("country", "Suppliers_country")
				.withColumnRenamed("phone", "Suppliers_phone")
				.withColumnRenamed("fax", "Suppliers_fax")
				.withColumnRenamed("homePage", "Suppliers_homePage")
				.withColumnRenamed("logEvents", "Suppliers_logEvents"),
				productsTDOsupplysuppliedProduct.col("relDB_ProductsInfo_supply_SupplierRef").equalTo(suppliersTDOsupplysupplier.col("relDB_ProductsInfo_supply_SupplierID")));
		Dataset<Products> res_Products_supply = res_supply.select( "productId", "productName", "quantityPerUnit", "unitPrice", "unitsInStock", "unitsOnOrder", "reorderLevel", "discontinued", "logEvents").as(Encoders.bean(Products.class));
		
		res_Products_supply = res_Products_supply.dropDuplicates(new String[] {"productId"});
		datasetsPOJO.add(res_Products_supply);
		
		
		Dataset<Supply> res_supply_suppliedProduct;
		Dataset<Products> res_Products;
		
		
		//Join datasets or return 
		Dataset<Products> res = fullOuterJoinsProducts(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Products>> lonelyProductsList = new ArrayList<Dataset<Products>>();
		lonelyProductsList.add(getProductsListInProductsStockInfoFromMyRedisDB(suppliedProduct_condition, new MutableBoolean(false)));
		Dataset<Products> lonelyProducts = fullOuterJoinsProducts(lonelyProductsList);
		if(lonelyProducts != null) {
			res = fullLeftOuterJoinsProducts(Arrays.asList(res, lonelyProducts));
		}
		if(suppliedProduct_refilter.booleanValue())
			res = res.filter((FilterFunction<Products>) r -> suppliedProduct_condition == null || suppliedProduct_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Products> getProductListInTypeOf(conditions.Condition<conditions.ProductsAttribute> product_condition,conditions.Condition<conditions.CategoriesAttribute> category_condition)		{
		MutableBoolean product_refilter = new MutableBoolean(false);
		List<Dataset<Products>> datasetsPOJO = new ArrayList<Dataset<Products>>();
		Dataset<Categories> all = null;
		boolean all_already_persisted = false;
		MutableBoolean category_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'product' in reference 'isCategory'. A->B Scenario
		category_refilter = new MutableBoolean(false);
		Dataset<ProductsTDO> productsTDOisCategoryproduct = typeOfService.getProductsTDOListProductInIsCategoryInProductsInfoFromRelDB(product_condition, product_refilter);
		Dataset<CategoriesTDO> categoriesTDOisCategorycategory = typeOfService.getCategoriesTDOListCategoryInIsCategoryInProductsInfoFromRelDB(category_condition, category_refilter);
		if(category_refilter.booleanValue()) {
			if(all == null)
				all = new CategoriesServiceImpl().getCategoriesList(category_condition);
			joinCondition = null;
			joinCondition = categoriesTDOisCategorycategory.col("categoryID").equalTo(all.col("categoryID"));
			if(joinCondition == null)
				categoriesTDOisCategorycategory = categoriesTDOisCategorycategory.as("A").join(all).select("A.*").as(Encoders.bean(CategoriesTDO.class));
			else
				categoriesTDOisCategorycategory = categoriesTDOisCategorycategory.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CategoriesTDO.class));
		}
	
		
		Dataset<Row> res_isCategory = productsTDOisCategoryproduct.join(categoriesTDOisCategorycategory
				.withColumnRenamed("categoryID", "Categories_categoryID")
				.withColumnRenamed("categoryName", "Categories_categoryName")
				.withColumnRenamed("description", "Categories_description")
				.withColumnRenamed("picture", "Categories_picture")
				.withColumnRenamed("logEvents", "Categories_logEvents"),
				productsTDOisCategoryproduct.col("relDB_ProductsInfo_isCategory_CategoryRef").equalTo(categoriesTDOisCategorycategory.col("relDB_ProductsInfo_isCategory_catid")));
		Dataset<Products> res_Products_isCategory = res_isCategory.select( "productId", "productName", "quantityPerUnit", "unitPrice", "unitsInStock", "unitsOnOrder", "reorderLevel", "discontinued", "logEvents").as(Encoders.bean(Products.class));
		
		res_Products_isCategory = res_Products_isCategory.dropDuplicates(new String[] {"productId"});
		datasetsPOJO.add(res_Products_isCategory);
		
		
		Dataset<TypeOf> res_typeOf_product;
		Dataset<Products> res_Products;
		
		
		//Join datasets or return 
		Dataset<Products> res = fullOuterJoinsProducts(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Products>> lonelyProductsList = new ArrayList<Dataset<Products>>();
		lonelyProductsList.add(getProductsListInProductsStockInfoFromMyRedisDB(product_condition, new MutableBoolean(false)));
		Dataset<Products> lonelyProducts = fullOuterJoinsProducts(lonelyProductsList);
		if(lonelyProducts != null) {
			res = fullLeftOuterJoinsProducts(Arrays.asList(res, lonelyProducts));
		}
		if(product_refilter.booleanValue())
			res = res.filter((FilterFunction<Products>) r -> product_condition == null || product_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Products> getOrderedProductsListInComposedOf(conditions.Condition<conditions.OrdersAttribute> order_condition,conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition, conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition)		{
		MutableBoolean orderedProducts_refilter = new MutableBoolean(false);
		List<Dataset<Products>> datasetsPOJO = new ArrayList<Dataset<Products>>();
		Dataset<Orders> all = null;
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		
		//join between 2 SQL tables and a non-relational structure
		// (A - AB) (B)
		order_refilter = new MutableBoolean(false);
		MutableBoolean composedOf_refilter = new MutableBoolean(false);
		Dataset<ComposedOfTDO> res_composedOf_purchasedProducts_order = composedOfService.getComposedOfTDOListInProductsInfoAndOrder_DetailsFrommyRelDB(orderedProducts_condition, composedOf_condition, orderedProducts_refilter, composedOf_refilter);
		Dataset<OrdersTDO> res_order_purchasedProducts = composedOfService.getOrdersTDOListOrderInOrderInOrdersFromMongoDB(order_condition, order_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
					all = new OrdersServiceImpl().getOrdersList(order_condition);
			joinCondition = null;
				joinCondition = res_order_purchasedProducts.col("id").equalTo(all.col("id"));
				res_order_purchasedProducts = res_order_purchasedProducts.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrdersTDO.class));
		}
		
		Dataset<Row> res_row_purchasedProducts_order = res_composedOf_purchasedProducts_order.join(res_order_purchasedProducts.withColumnRenamed("logEvents", "composedOf_logEvents"),
																														res_composedOf_purchasedProducts_order.col("relDB_Order_Details_order_OrderRef").equalTo(res_order_purchasedProducts.col("relDB_Order_Details_order_OrderID")));																												
																														
		Dataset<Products> res_Products_purchasedProducts = res_row_purchasedProducts_order.select("orderedProducts.*").as(Encoders.bean(Products.class));
		datasetsPOJO.add(res_Products_purchasedProducts.dropDuplicates(new String[] {"productId"}));	
		
		
		
		Dataset<ComposedOf> res_composedOf_orderedProducts;
		Dataset<Products> res_Products;
		
		
		//Join datasets or return 
		Dataset<Products> res = fullOuterJoinsProducts(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Products>> lonelyProductsList = new ArrayList<Dataset<Products>>();
		lonelyProductsList.add(getProductsListInProductsStockInfoFromMyRedisDB(orderedProducts_condition, new MutableBoolean(false)));
		Dataset<Products> lonelyProducts = fullOuterJoinsProducts(lonelyProductsList);
		if(lonelyProducts != null) {
			res = fullLeftOuterJoinsProducts(Arrays.asList(res, lonelyProducts));
		}
		if(orderedProducts_refilter.booleanValue())
			res = res.filter((FilterFunction<Products>) r -> orderedProducts_condition == null || orderedProducts_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertProducts(Products products){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertProductsInProductsStockInfoFromMyRedisDB(products) || inserted ;
			inserted = insertProductsInProductsInfoFromMyRelDB(products) || inserted ;
		return inserted;
	}
	
	public boolean insertProductsInProductsStockInfoFromMyRedisDB(Products products)	{
		String idvalue="";
		idvalue+=products.getProductId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "PRODUCT:";
			key += products.getProductId();
			key += ":STOCKINFO";
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_UnitsInStock="UnitsInStock";
			String _value_UnitsInStock="";
			if(products.getUnitsInStock()!=null){
				toAdd = true;
				_value_UnitsInStock += products.getUnitsInStock();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsInStock,_value_UnitsInStock));
			toAdd = false;
			String _fieldname_UnitsOnOrder="UnitsOnOrder";
			String _value_UnitsOnOrder="";
			if(products.getUnitsOnOrder()!=null){
				toAdd = true;
				_value_UnitsOnOrder += products.getUnitsOnOrder();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsOnOrder,_value_UnitsOnOrder));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "myRedisDB");
	
			logger.info("Inserted [Products] entity ID [{}] in [ProductsStockInfo] in database [MyRedisDB]", idvalue);
		}
		else
			logger.warn("[Products] entity ID [{}] already present in [ProductsStockInfo] in database [MyRedisDB]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertProductsInProductsInfoFromMyRelDB(Products products)	{
		String idvalue="";
		idvalue+=products.getProductId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("ProductID");
		values.add(products.getProductId());
		columns.add("ProductName");
		values.add(products.getProductName());
		columns.add("QuantityPerUnit");
		values.add(products.getQuantityPerUnit());
		columns.add("UnitPrice");
		values.add(products.getUnitPrice());
		columns.add("ReorderLevel");
		values.add(products.getReorderLevel());
		columns.add("Discontinued");
		values.add(products.getDiscontinued());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "ProductsInfo", "myRelDB");
			logger.info("Inserted [Products] entity ID [{}] in [ProductsInfo] in database [MyRelDB]", idvalue);
		}
		else
			logger.warn("[Products] entity ID [{}] already present in [ProductsInfo] in database [MyRelDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allProductsIdList = null;
	public void updateProductsList(conditions.Condition<conditions.ProductsAttribute> condition, conditions.SetClause<conditions.ProductsAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInProductsStockInfoFromMyRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInProductsStockInfoFromMyRedisDB.booleanValue())
				updateProductsListInProductsStockInfoFromMyRedisDB(condition, set);
		
			MutableBoolean refilterInProductsInfoFromMyRelDB = new MutableBoolean(false);
			getSQLWhereClauseInProductsInfoFromMyRelDB(condition, refilterInProductsInfoFromMyRelDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInProductsInfoFromMyRelDB.booleanValue())
				updateProductsListInProductsInfoFromMyRelDB(condition, set);
		
	
			if(!refilterInProductsStockInfoFromMyRedisDB.booleanValue())
				updateProductsListInProductsStockInfoFromMyRedisDB(condition, set);
			if(!refilterInProductsInfoFromMyRelDB.booleanValue())
				updateProductsListInProductsInfoFromMyRelDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateProductsListInProductsStockInfoFromMyRedisDB(Condition<ProductsAttribute> condition, SetClause<ProductsAttribute> set) {
		//TODO
	}
	public void updateProductsListInProductsInfoFromMyRelDB(Condition<ProductsAttribute> condition, SetClause<ProductsAttribute> set) {
		List<String> setClause = ProductsServiceImpl.getSQLSetClauseInProductsInfoFromMyRelDB(set);
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
		Pair<String, List<String>> whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsInfoFromMyRelDB(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE ProductsInfo SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "myRelDB");
		} else {
			if(!inUpdateMethod || allProductsIdList == null)
				allProductsIdList = this.getProductsList(condition).select("productId").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allProductsIdList) {
				Condition<ProductsAttribute> conditionId = null;
				conditionId = Condition.simple(ProductsAttribute.productId, Operator.EQUALS, row.getAs("productId"));
				whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsInfoFromMyRelDB(conditionId, refilter);
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
		
			DBConnectionMgr.updatesInTable(updateQueries, "myRelDB");
		}
		
	}
	
	
	
	public void updateProducts(pojo.Products products) {
		//TODO using the id
		return;
	}
	public void updateSuppliedProductListInSupply(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		//TODO
	}
	
	public void updateSuppliedProductListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateSuppliedProductListInSupply(suppliedProduct_condition, null, set);
	}
	public void updateSuppliedProductListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateSuppliedProductListInSupply(null, supplier_condition, set);
	}
	
	public void updateSuppliedProductListInSupplyBySupplier(
		pojo.Suppliers supplier,
		conditions.SetClause<conditions.ProductsAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateProductListInTypeOf(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		//TODO
	}
	
	public void updateProductListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateProductListInTypeOf(product_condition, null, set);
	}
	public void updateProductListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateProductListInTypeOf(null, category_condition, set);
	}
	
	public void updateProductListInTypeOfByCategory(
		pojo.Categories category,
		conditions.SetClause<conditions.ProductsAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderedProductsListInComposedOf(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		//TODO
	}
	
	public void updateOrderedProductsListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateOrderedProductsListInComposedOf(order_condition, null, null, set);
	}
	
	public void updateOrderedProductsListInComposedOfByOrder(
		pojo.Orders order,
		conditions.SetClause<conditions.ProductsAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderedProductsListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateOrderedProductsListInComposedOf(null, orderedProducts_condition, null, set);
	}
	public void updateOrderedProductsListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateOrderedProductsListInComposedOf(null, null, composedOf_condition, set);
	}
	
	
	public void deleteProductsList(conditions.Condition<conditions.ProductsAttribute> condition){
		//TODO
	}
	
	public void deleteProducts(pojo.Products products) {
		//TODO using the id
		return;
	}
	public void deleteSuppliedProductListInSupply(	
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,	
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition){
			//TODO
		}
	
	public void deleteSuppliedProductListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition
	){
		deleteSuppliedProductListInSupply(suppliedProduct_condition, null);
	}
	public void deleteSuppliedProductListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition
	){
		deleteSuppliedProductListInSupply(null, supplier_condition);
	}
	
	public void deleteSuppliedProductListInSupplyBySupplier(
		pojo.Suppliers supplier 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteProductListInTypeOf(	
		conditions.Condition<conditions.ProductsAttribute> product_condition,	
		conditions.Condition<conditions.CategoriesAttribute> category_condition){
			//TODO
		}
	
	public void deleteProductListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition
	){
		deleteProductListInTypeOf(product_condition, null);
	}
	public void deleteProductListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition
	){
		deleteProductListInTypeOf(null, category_condition);
	}
	
	public void deleteProductListInTypeOfByCategory(
		pojo.Categories category 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderedProductsListInComposedOf(	
		conditions.Condition<conditions.OrdersAttribute> order_condition,	
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf){
			//TODO
		}
	
	public void deleteOrderedProductsListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition
	){
		deleteOrderedProductsListInComposedOf(order_condition, null, null);
	}
	
	public void deleteOrderedProductsListInComposedOfByOrder(
		pojo.Orders order 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderedProductsListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition
	){
		deleteOrderedProductsListInComposedOf(null, orderedProducts_condition, null);
	}
	public void deleteOrderedProductsListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		deleteOrderedProductsListInComposedOf(null, null, composedOf_condition);
	}
	
}
