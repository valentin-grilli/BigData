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
import conditions.TypeOfAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ProductsTDO;
import tdo.TypeOfTDO;
import conditions.ProductsAttribute;
import dao.services.ProductsService;
import tdo.CategoriesTDO;
import tdo.TypeOfTDO;
import conditions.CategoriesAttribute;
import dao.services.CategoriesService;
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

public class TypeOfServiceImpl extends dao.services.TypeOfService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeOfServiceImpl.class);
	
	
	// Left side 'CategoryRef' of reference [isCategory ]
	public Dataset<ProductsTDO> getProductsTDOListProductInIsCategoryInProductsInfoFromRelDB(Condition<ProductsAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsInfoFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "ProductsInfo", where);
		
	
		Dataset<ProductsTDO> res = d.map((MapFunction<Row, ProductsTDO>) r -> {
					ProductsTDO products_res = new ProductsTDO();
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
	
					// Get reference column [CategoryRef ] for reference [isCategory]
					String relDB_ProductsInfo_isCategory_CategoryRef = r.getAs("CategoryRef") == null ? null : r.getAs("CategoryRef").toString();
					products_res.setRelDB_ProductsInfo_isCategory_CategoryRef(relDB_ProductsInfo_isCategory_CategoryRef);
	
	
					return products_res;
				}, Encoders.bean(ProductsTDO.class));
	
	
		return res;
	}
	
	// Right side 'catid' of reference [isCategory ]
	public Dataset<CategoriesTDO> getCategoriesTDOListCategoryInIsCategoryInProductsInfoFromRelDB(Condition<CategoriesAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<CategoriesAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("CATEGORY:");
		keypatternAllVariables=keypatternAllVariables.concat("CATEGORY:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,CategoriesAttribute.categoryID));
			keyAttributes.add(CategoriesAttribute.categoryID);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("catid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<CategoriesAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (CategoriesAttribute a : conditionAttributes) {
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
			DataTypes.createStructField("CategoryName", DataTypes.StringType, true)
	,		DataTypes.createStructField("Description", DataTypes.StringType, true)
	,		DataTypes.createStructField("Picture", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myRedisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<CategoriesTDO> res = rows.map((MapFunction<Row, CategoriesTDO>) r -> {
					CategoriesTDO categories_res = new CategoriesTDO();
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
					// attribute [Categories.CategoryID]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("catid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Categories' mapped physical field 'catid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String categoryID = null;
					if(matches) {
						categoryID = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for CategoriescategoryID attribute stored in db myRedisDB. Regex [{}] Value [{}]",regex,value);
						categories_res.addLogEvent("Cannot retrieve value for Categories.categoryID attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
					}
					categories_res.setCategoryID(categoryID == null ? null : Integer.parseInt(categoryID));
					// attribute [Categories.CategoryName]
					String categoryName = r.getAs("CategoryName") == null ? null : r.getAs("CategoryName");
					categories_res.setCategoryName(categoryName);
					// attribute [Categories.Description]
					String description = r.getAs("Description") == null ? null : r.getAs("Description");
					categories_res.setDescription(description);
					// attribute [Categories.Picture]
					byte[] picture = r.getAs("Picture") == null ? null : ((String)r.getAs("Picture")).getBytes();
					categories_res.setPicture(picture);
					//Checking that reference field 'catid' is mapped in Key
					if(fieldsListInKey.contains("catid")){
						//Retrieving reference field 'catid' in Key
						Pattern pattern_catid = Pattern.compile("\\*");
				        Matcher match_catid = pattern_catid.matcher(finalKeypattern);
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("catid")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Categories' mapped physical field 'catid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String isCategory_catid = null;
						if(matches) {
						isCategory_catid = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'catid'. Regex [{}] Value [{}]",regex,value);
						categories_res.addLogEvent("Cannot retrieve value for 'catid' attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
						}
						categories_res.setRelDB_ProductsInfo_isCategory_catid(isCategory_catid);
					}
	
						return categories_res;
				}, Encoders.bean(CategoriesTDO.class));
		res=res.dropDuplicates(new String[] {"categoryID"});
		return res;
	}
	
	
	
	
	public Dataset<TypeOf> getTypeOfList(
		Condition<ProductsAttribute> product_condition,
		Condition<CategoriesAttribute> category_condition){
			TypeOfServiceImpl typeOfService = this;
			ProductsService productsService = new ProductsServiceImpl();  
			CategoriesService categoriesService = new CategoriesServiceImpl();
			MutableBoolean product_refilter = new MutableBoolean(false);
			List<Dataset<TypeOf>> datasetsPOJO = new ArrayList<Dataset<TypeOf>>();
			boolean all_already_persisted = false;
			MutableBoolean category_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'product' in reference 'isCategory'. A->B Scenario
			category_refilter = new MutableBoolean(false);
			Dataset<ProductsTDO> productsTDOisCategoryproduct = typeOfService.getProductsTDOListProductInIsCategoryInProductsInfoFromRelDB(product_condition, product_refilter);
			Dataset<CategoriesTDO> categoriesTDOisCategorycategory = typeOfService.getCategoriesTDOListCategoryInIsCategoryInProductsInfoFromRelDB(category_condition, category_refilter);
			
			Dataset<Row> res_isCategory_temp = productsTDOisCategoryproduct.join(categoriesTDOisCategorycategory
					.withColumnRenamed("categoryID", "Categories_categoryID")
					.withColumnRenamed("categoryName", "Categories_categoryName")
					.withColumnRenamed("description", "Categories_description")
					.withColumnRenamed("picture", "Categories_picture")
					.withColumnRenamed("logEvents", "Categories_logEvents"),
					productsTDOisCategoryproduct.col("relDB_ProductsInfo_isCategory_CategoryRef").equalTo(categoriesTDOisCategorycategory.col("relDB_ProductsInfo_isCategory_catid")));
		
			Dataset<TypeOf> res_isCategory = res_isCategory_temp.map(
				(MapFunction<Row, TypeOf>) r -> {
					TypeOf res = new TypeOf();
					Products A = new Products();
					Categories B = new Categories();
					A.setProductId(Util.getIntegerValue(r.getAs("productId")));
					A.setProductName(Util.getStringValue(r.getAs("productName")));
					A.setQuantityPerUnit(Util.getStringValue(r.getAs("quantityPerUnit")));
					A.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					A.setUnitsInStock(Util.getIntegerValue(r.getAs("unitsInStock")));
					A.setUnitsOnOrder(Util.getIntegerValue(r.getAs("unitsOnOrder")));
					A.setReorderLevel(Util.getIntegerValue(r.getAs("reorderLevel")));
					A.setDiscontinued(Util.getBooleanValue(r.getAs("discontinued")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setCategoryID(Util.getIntegerValue(r.getAs("Categories_categoryID")));
					B.setCategoryName(Util.getStringValue(r.getAs("Categories_categoryName")));
					B.setDescription(Util.getStringValue(r.getAs("Categories_description")));
					B.setPicture(Util.getByteArrayValue(r.getAs("Categories_picture")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Categories_logEvents")));
						
					res.setProduct(A);
					res.setCategory(B);
					return res;
				},Encoders.bean(TypeOf.class)
			);
		
			datasetsPOJO.add(res_isCategory);
		
			
			Dataset<TypeOf> res_typeOf_product;
			Dataset<Products> res_Products;
			
			
			//Join datasets or return 
			Dataset<TypeOf> res = fullOuterJoinsTypeOf(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Products> lonelyProduct = null;
			Dataset<Categories> lonelyCategory = null;
			
			List<Dataset<Products>> lonelyproductList = new ArrayList<Dataset<Products>>();
			lonelyproductList.add(productsService.getProductsListInProductsStockInfoFromMyRedisDB(product_condition, new MutableBoolean(false)));
			lonelyProduct = ProductsService.fullOuterJoinsProducts(lonelyproductList);
			if(lonelyProduct != null) {
				res = fullLeftOuterJoinBetweenTypeOfAndProduct(res, lonelyProduct);
			}	
		
		
			
			if(product_refilter.booleanValue() || category_refilter.booleanValue())
				res = res.filter((FilterFunction<TypeOf>) r -> (product_condition == null || product_condition.evaluate(r.getProduct())) && (category_condition == null || category_condition.evaluate(r.getCategory())));
			
		
			return res;
		
		}
	
	public Dataset<TypeOf> getTypeOfListByProductCondition(
		Condition<ProductsAttribute> product_condition
	){
		return getTypeOfList(product_condition, null);
	}
	
	public TypeOf getTypeOfByProduct(Products product) {
		Condition<ProductsAttribute> cond = null;
		cond = Condition.simple(ProductsAttribute.productId, Operator.EQUALS, product.getProductId());
		Dataset<TypeOf> res = getTypeOfListByProductCondition(cond);
		List<TypeOf> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<TypeOf> getTypeOfListByCategoryCondition(
		Condition<CategoriesAttribute> category_condition
	){
		return getTypeOfList(null, category_condition);
	}
	
	public Dataset<TypeOf> getTypeOfListByCategory(Categories category) {
		Condition<CategoriesAttribute> cond = null;
		cond = Condition.simple(CategoriesAttribute.categoryID, Operator.EQUALS, category.getCategoryID());
		Dataset<TypeOf> res = getTypeOfListByCategoryCondition(cond);
	return res;
	}
	
	public void insertTypeOf(TypeOf typeOf){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertTypeOfInRefStructProductsInfoInMyRelDB(typeOf);
	}
	
	
	
	public 	boolean insertTypeOfInRefStructProductsInfoInMyRelDB(TypeOf typeOf){
	 	// Rel 'typeOf' Insert in reference structure 'ProductsInfo'
		Products productsProduct = typeOf.getProduct();
		Categories categoriesCategory = typeOf.getCategory();
	
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		String filtercolumn;
		Object filtervalue;
		columns.add("CategoryRef");
		values.add(categoriesCategory==null?null:categoriesCategory.getCategoryID());
		filtercolumn = "ProductID";
		filtervalue = productsProduct.getProductId();
		DBConnectionMgr.updateInTable(filtercolumn, filtervalue, columns, values, "ProductsInfo", "myRelDB");					
		return true;
	}
	
	
	
	
	public void deleteTypeOfList(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition){
			//TODO
		}
	
	public void deleteTypeOfListByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition
	){
		deleteTypeOfList(product_condition, null);
	}
	
	public void deleteTypeOfByProduct(pojo.Products product) {
		// TODO using id for selecting
		return;
	}
	public void deleteTypeOfListByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition
	){
		deleteTypeOfList(null, category_condition);
	}
	
	public void deleteTypeOfListByCategory(pojo.Categories category) {
		// TODO using id for selecting
		return;
	}
		
}
