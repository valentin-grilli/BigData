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
import conditions.Belongs_toAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ProductTDO;
import tdo.Belongs_toTDO;
import conditions.ProductAttribute;
import dao.services.ProductService;
import tdo.CategoryTDO;
import tdo.Belongs_toTDO;
import conditions.CategoryAttribute;
import dao.services.CategoryService;
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

public class Belongs_toServiceImpl extends dao.services.Belongs_toService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Belongs_toServiceImpl.class);
	
	
	// Left side 'CategoryRef' of reference [categoryR ]
	public Dataset<ProductTDO> getProductTDOListProductInCategoryRInProductsInfoFromRelSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "ProductsInfo", where);
		
	
		Dataset<ProductTDO> res = d.map((MapFunction<Row, ProductTDO>) r -> {
					ProductTDO product_res = new ProductTDO();
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
	
					// Get reference column [CategoryRef ] for reference [categoryR]
					String relSchema_ProductsInfo_categoryR_CategoryRef = r.getAs("CategoryRef") == null ? null : r.getAs("CategoryRef").toString();
					product_res.setRelSchema_ProductsInfo_categoryR_CategoryRef(relSchema_ProductsInfo_categoryR_CategoryRef);
	
	
					return product_res;
				}, Encoders.bean(ProductTDO.class));
	
	
		return res;
	}
	
	// Right side 'categoryid' of reference [categoryR ]
	public Dataset<CategoryTDO> getCategoryTDOListCategoryInCategoryRInProductsInfoFromRelSchema(Condition<CategoryAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<CategoryAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("CATEGORY:");
		keypatternAllVariables=keypatternAllVariables.concat("CATEGORY:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,CategoryAttribute.id));
			keyAttributes.add(CategoryAttribute.id);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("categoryid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<CategoryAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (CategoryAttribute a : conditionAttributes) {
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
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("redisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<CategoryTDO> res = rows.map((MapFunction<Row, CategoryTDO>) r -> {
					CategoryTDO category_res = new CategoryTDO();
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
					// attribute [Category.Id]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("categoryid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Category' mapped physical field 'categoryid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Categoryid attribute stored in db redisDB. Regex [{}] Value [{}]",regex,value);
						category_res.addLogEvent("Cannot retrieve value for Category.id attribute stored in db redisDB. Probably due to an ambiguous regex.");
					}
					category_res.setId(id == null ? null : Integer.parseInt(id));
					// attribute [Category.CategoryName]
					String categoryName = r.getAs("CategoryName") == null ? null : r.getAs("CategoryName");
					category_res.setCategoryName(categoryName);
					// attribute [Category.Description]
					String description = r.getAs("Description") == null ? null : r.getAs("Description");
					category_res.setDescription(description);
					// attribute [Category.Picture]
					String picture = r.getAs("Picture") == null ? null : r.getAs("Picture");
					category_res.setPicture(picture);
					//Checking that reference field 'categoryid' is mapped in Key
					if(fieldsListInKey.contains("categoryid")){
						//Retrieving reference field 'categoryid' in Key
						Pattern pattern_categoryid = Pattern.compile("\\*");
				        Matcher match_categoryid = pattern_categoryid.matcher(finalKeypattern);
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("categoryid")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Category' mapped physical field 'categoryid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String categoryR_categoryid = null;
						if(matches) {
						categoryR_categoryid = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'categoryid'. Regex [{}] Value [{}]",regex,value);
						category_res.addLogEvent("Cannot retrieve value for 'categoryid' attribute stored in db redisDB. Probably due to an ambiguous regex.");
						}
						category_res.setRelSchema_ProductsInfo_categoryR_categoryid(categoryR_categoryid);
					}
	
						return category_res;
				}, Encoders.bean(CategoryTDO.class));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
	}
	
	
	
	
	public Dataset<Belongs_to> getBelongs_toList(
		Condition<ProductAttribute> product_condition,
		Condition<CategoryAttribute> category_condition){
			Belongs_toServiceImpl belongs_toService = this;
			ProductService productService = new ProductServiceImpl();  
			CategoryService categoryService = new CategoryServiceImpl();
			MutableBoolean product_refilter = new MutableBoolean(false);
			List<Dataset<Belongs_to>> datasetsPOJO = new ArrayList<Dataset<Belongs_to>>();
			boolean all_already_persisted = false;
			MutableBoolean category_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'product' in reference 'categoryR'. A->B Scenario
			category_refilter = new MutableBoolean(false);
			Dataset<ProductTDO> productTDOcategoryRproduct = belongs_toService.getProductTDOListProductInCategoryRInProductsInfoFromRelSchema(product_condition, product_refilter);
			Dataset<CategoryTDO> categoryTDOcategoryRcategory = belongs_toService.getCategoryTDOListCategoryInCategoryRInProductsInfoFromRelSchema(category_condition, category_refilter);
			
			Dataset<Row> res_categoryR_temp = productTDOcategoryRproduct.join(categoryTDOcategoryRcategory
					.withColumnRenamed("id", "Category_id")
					.withColumnRenamed("categoryName", "Category_categoryName")
					.withColumnRenamed("description", "Category_description")
					.withColumnRenamed("picture", "Category_picture")
					.withColumnRenamed("logEvents", "Category_logEvents"),
					productTDOcategoryRproduct.col("relSchema_ProductsInfo_categoryR_CategoryRef").equalTo(categoryTDOcategoryRcategory.col("relSchema_ProductsInfo_categoryR_categoryid")));
		
			Dataset<Belongs_to> res_categoryR = res_categoryR_temp.map(
				(MapFunction<Row, Belongs_to>) r -> {
					Belongs_to res = new Belongs_to();
					Product A = new Product();
					Category B = new Category();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setName(Util.getStringValue(r.getAs("name")));
					A.setSupplierRef(Util.getIntegerValue(r.getAs("supplierRef")));
					A.setCategoryRef(Util.getIntegerValue(r.getAs("categoryRef")));
					A.setQuantityPerUnit(Util.getStringValue(r.getAs("quantityPerUnit")));
					A.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					A.setReorderLevel(Util.getIntegerValue(r.getAs("reorderLevel")));
					A.setDiscontinued(Util.getBooleanValue(r.getAs("discontinued")));
					A.setUnitsInStock(Util.getIntegerValue(r.getAs("unitsInStock")));
					A.setUnitsOnOrder(Util.getIntegerValue(r.getAs("unitsOnOrder")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("Category_id")));
					B.setCategoryName(Util.getStringValue(r.getAs("Category_categoryName")));
					B.setDescription(Util.getStringValue(r.getAs("Category_description")));
					B.setPicture(Util.getStringValue(r.getAs("Category_picture")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Category_logEvents")));
						
					res.setProduct(A);
					res.setCategory(B);
					return res;
				},Encoders.bean(Belongs_to.class)
			);
		
			datasetsPOJO.add(res_categoryR);
		
			
			Dataset<Belongs_to> res_belongs_to_product;
			Dataset<Product> res_Product;
			
			
			//Join datasets or return 
			Dataset<Belongs_to> res = fullOuterJoinsBelongs_to(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Product> lonelyProduct = null;
			Dataset<Category> lonelyCategory = null;
			
			List<Dataset<Product>> lonelyproductList = new ArrayList<Dataset<Product>>();
			lonelyproductList.add(productService.getProductListInStockInfoPairsFromRedisDB(product_condition, new MutableBoolean(false)));
			lonelyProduct = ProductService.fullOuterJoinsProduct(lonelyproductList);
			if(lonelyProduct != null) {
				res = fullLeftOuterJoinBetweenBelongs_toAndProduct(res, lonelyProduct);
			}	
		
		
			
			if(product_refilter.booleanValue() || category_refilter.booleanValue())
				res = res.filter((FilterFunction<Belongs_to>) r -> (product_condition == null || product_condition.evaluate(r.getProduct())) && (category_condition == null || category_condition.evaluate(r.getCategory())));
			
		
			return res;
		
		}
	
	public Dataset<Belongs_to> getBelongs_toListByProductCondition(
		Condition<ProductAttribute> product_condition
	){
		return getBelongs_toList(product_condition, null);
	}
	
	public Belongs_to getBelongs_toByProduct(Product product) {
		Condition<ProductAttribute> cond = null;
		cond = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		Dataset<Belongs_to> res = getBelongs_toListByProductCondition(cond);
		List<Belongs_to> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Belongs_to> getBelongs_toListByCategoryCondition(
		Condition<CategoryAttribute> category_condition
	){
		return getBelongs_toList(null, category_condition);
	}
	
	public Dataset<Belongs_to> getBelongs_toListByCategory(Category category) {
		Condition<CategoryAttribute> cond = null;
		cond = Condition.simple(CategoryAttribute.id, Operator.EQUALS, category.getId());
		Dataset<Belongs_to> res = getBelongs_toListByCategoryCondition(cond);
	return res;
	}
	
	
	
	public void deleteBelongs_toList(
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.CategoryAttribute> category_condition){
			//TODO
		}
	
	public void deleteBelongs_toListByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteBelongs_toList(product_condition, null);
	}
	
	public void deleteBelongs_toByProduct(pojo.Product product) {
		// TODO using id for selecting
		return;
	}
	public void deleteBelongs_toListByCategoryCondition(
		conditions.Condition<conditions.CategoryAttribute> category_condition
	){
		deleteBelongs_toList(null, category_condition);
	}
	
	public void deleteBelongs_toListByCategory(pojo.Category category) {
		// TODO using id for selecting
		return;
	}
		
}
