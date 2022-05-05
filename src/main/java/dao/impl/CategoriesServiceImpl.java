package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Categories;
import conditions.*;
import dao.services.CategoriesService;
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


public class CategoriesServiceImpl extends CategoriesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CategoriesServiceImpl.class);
	
	
	
	
	
	
	
	//TODO redis
	public Dataset<Categories> getCategoriesListInCategoriesKVFromMyRedisDB(conditions.Condition<conditions.CategoriesAttribute> condition, MutableBoolean refilterFlag){
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
		Dataset<Categories> res = rows.map((MapFunction<Row, Categories>) r -> {
					Categories categories_res = new Categories();
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
	
						return categories_res;
				}, Encoders.bean(Categories.class));
		res=res.dropDuplicates(new String[] {"categoryID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Categories> getCategoryListInTypeOf(conditions.Condition<conditions.ProductsAttribute> product_condition,conditions.Condition<conditions.CategoriesAttribute> category_condition)		{
		MutableBoolean category_refilter = new MutableBoolean(false);
		List<Dataset<Categories>> datasetsPOJO = new ArrayList<Dataset<Categories>>();
		Dataset<Products> all = null;
		boolean all_already_persisted = false;
		MutableBoolean product_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		product_refilter = new MutableBoolean(false);
		// For role 'product' in reference 'isCategory'  B->A Scenario
		Dataset<ProductsTDO> productsTDOisCategoryproduct = typeOfService.getProductsTDOListProductInIsCategoryInProductsInfoFromRelDB(product_condition, product_refilter);
		Dataset<CategoriesTDO> categoriesTDOisCategorycategory = typeOfService.getCategoriesTDOListCategoryInIsCategoryInProductsInfoFromRelDB(category_condition, category_refilter);
		if(product_refilter.booleanValue()) {
			if(all == null)
				all = new ProductsServiceImpl().getProductsList(product_condition);
			joinCondition = null;
			joinCondition = productsTDOisCategoryproduct.col("productId").equalTo(all.col("productId"));
			if(joinCondition == null)
				productsTDOisCategoryproduct = productsTDOisCategoryproduct.as("A").join(all).select("A.*").as(Encoders.bean(ProductsTDO.class));
			else
				productsTDOisCategoryproduct = productsTDOisCategoryproduct.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductsTDO.class));
		}
		Dataset<Row> res_isCategory = 
			categoriesTDOisCategorycategory.join(productsTDOisCategoryproduct
				.withColumnRenamed("productId", "Products_productId")
				.withColumnRenamed("productName", "Products_productName")
				.withColumnRenamed("quantityPerUnit", "Products_quantityPerUnit")
				.withColumnRenamed("unitPrice", "Products_unitPrice")
				.withColumnRenamed("unitsInStock", "Products_unitsInStock")
				.withColumnRenamed("unitsOnOrder", "Products_unitsOnOrder")
				.withColumnRenamed("reorderLevel", "Products_reorderLevel")
				.withColumnRenamed("discontinued", "Products_discontinued")
				.withColumnRenamed("logEvents", "Products_logEvents"),
				categoriesTDOisCategorycategory.col("relDB_ProductsInfo_isCategory_catid").equalTo(productsTDOisCategoryproduct.col("relDB_ProductsInfo_isCategory_CategoryRef")));
		Dataset<Categories> res_Categories_isCategory = res_isCategory.select( "categoryID", "categoryName", "description", "picture", "logEvents").as(Encoders.bean(Categories.class));
		res_Categories_isCategory = res_Categories_isCategory.dropDuplicates(new String[] {"categoryID"});
		datasetsPOJO.add(res_Categories_isCategory);
		
		Dataset<TypeOf> res_typeOf_category;
		Dataset<Categories> res_Categories;
		
		
		//Join datasets or return 
		Dataset<Categories> res = fullOuterJoinsCategories(datasetsPOJO);
		if(res == null)
			return null;
	
		if(category_refilter.booleanValue())
			res = res.filter((FilterFunction<Categories>) r -> category_condition == null || category_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertCategories(Categories categories){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCategoriesInCategoriesKVFromMyRedisDB(categories) || inserted ;
		return inserted;
	}
	
	public boolean insertCategoriesInCategoriesKVFromMyRedisDB(Categories categories)	{
		String idvalue="";
		idvalue+=categories.getCategoryID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "CATEGORY:";
			key += categories.getCategoryID();
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_CategoryName="CategoryName";
			String _value_CategoryName="";
			if(categories.getCategoryName()!=null){
				toAdd = true;
				_value_CategoryName += categories.getCategoryName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_CategoryName,_value_CategoryName));
			toAdd = false;
			String _fieldname_Description="Description";
			String _value_Description="";
			if(categories.getDescription()!=null){
				toAdd = true;
				_value_Description += categories.getDescription();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Description,_value_Description));
			toAdd = false;
			String _fieldname_Picture="Picture";
			String _value_Picture="";
			if(categories.getPicture()!=null){
				toAdd = true;
				_value_Picture += categories.getPicture();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Picture,_value_Picture));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "myRedisDB");
	
			logger.info("Inserted [Categories] entity ID [{}] in [CategoriesKV] in database [MyRedisDB]", idvalue);
		}
		else
			logger.warn("[Categories] entity ID [{}] already present in [CategoriesKV] in database [MyRedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allCategoriesIdList = null;
	public void updateCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition, conditions.SetClause<conditions.CategoriesAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInCategoriesKVFromMyRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInCategoriesKVFromMyRedisDB.booleanValue())
				updateCategoriesListInCategoriesKVFromMyRedisDB(condition, set);
		
	
			if(!refilterInCategoriesKVFromMyRedisDB.booleanValue())
				updateCategoriesListInCategoriesKVFromMyRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateCategoriesListInCategoriesKVFromMyRedisDB(Condition<CategoriesAttribute> condition, SetClause<CategoriesAttribute> set) {
		//TODO
	}
	
	
	
	public void updateCategories(pojo.Categories categories) {
		//TODO using the id
		return;
	}
	public void updateCategoryListInTypeOf(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		
		conditions.SetClause<conditions.CategoriesAttribute> set
	){
		//TODO
	}
	
	public void updateCategoryListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.SetClause<conditions.CategoriesAttribute> set
	){
		updateCategoryListInTypeOf(product_condition, null, set);
	}
	
	public void updateCategoryInTypeOfByProduct(
		pojo.Products product,
		conditions.SetClause<conditions.CategoriesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateCategoryListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		conditions.SetClause<conditions.CategoriesAttribute> set
	){
		updateCategoryListInTypeOf(null, category_condition, set);
	}
	
	
	public void deleteCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition){
		//TODO
	}
	
	public void deleteCategories(pojo.Categories categories) {
		//TODO using the id
		return;
	}
	public void deleteCategoryListInTypeOf(	
		conditions.Condition<conditions.ProductsAttribute> product_condition,	
		conditions.Condition<conditions.CategoriesAttribute> category_condition){
			//TODO
		}
	
	public void deleteCategoryListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition
	){
		deleteCategoryListInTypeOf(product_condition, null);
	}
	
	public void deleteCategoryInTypeOfByProduct(
		pojo.Products product 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteCategoryListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition
	){
		deleteCategoryListInTypeOf(null, category_condition);
	}
	
}
