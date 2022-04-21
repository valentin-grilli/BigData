package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Category;
import conditions.*;
import dao.services.CategoryService;
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


public class CategoryServiceImpl extends CategoryService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CategoryServiceImpl.class);
	
	
	
	
	
	
	
	//TODO redis
	public Dataset<Category> getCategoryListInCategoryPairsFromRedisDB(conditions.Condition<conditions.CategoryAttribute> condition, MutableBoolean refilterFlag){
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
		Dataset<Category> res = rows.map((MapFunction<Row, Category>) r -> {
					Category category_res = new Category();
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
	
						return category_res;
				}, Encoders.bean(Category.class));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
		
	}
	
	
	
	
	
	
	
	
	public boolean insertCategory(Category category){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCategoryInCategoryPairsFromRedisDB(category) || inserted ;
		return inserted;
	}
	
	public boolean insertCategoryInCategoryPairsFromRedisDB(Category category)	{
		String idvalue="";
		idvalue+=category.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "CATEGORY:";
			key += category.getId();
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_CategoryName="CategoryName";
			String _value_CategoryName="";
			if(category.getCategoryName()!=null){
				toAdd = true;
				_value_CategoryName += category.getCategoryName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_CategoryName,_value_CategoryName));
			toAdd = false;
			String _fieldname_Description="Description";
			String _value_Description="";
			if(category.getDescription()!=null){
				toAdd = true;
				_value_Description += category.getDescription();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Description,_value_Description));
			toAdd = false;
			String _fieldname_Picture="Picture";
			String _value_Picture="";
			if(category.getPicture()!=null){
				toAdd = true;
				_value_Picture += category.getPicture();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Picture,_value_Picture));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "redisDB");
	
			logger.info("Inserted [Category] entity ID [{}] in [CategoryPairs] in database [RedisDB]", idvalue);
		}
		else
			logger.warn("[Category] entity ID [{}] already present in [CategoryPairs] in database [RedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allCategoryIdList = null;
	public void updateCategoryList(conditions.Condition<conditions.CategoryAttribute> condition, conditions.SetClause<conditions.CategoryAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInCategoryPairsFromRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInCategoryPairsFromRedisDB.booleanValue())
				updateCategoryListInCategoryPairsFromRedisDB(condition, set);
		
	
			if(!refilterInCategoryPairsFromRedisDB.booleanValue())
				updateCategoryListInCategoryPairsFromRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateCategoryListInCategoryPairsFromRedisDB(Condition<CategoryAttribute> condition, SetClause<CategoryAttribute> set) {
		//TODO
	}
	
	
	
	public void updateCategory(pojo.Category category) {
		//TODO using the id
		return;
	}
	
	
	public void deleteCategoryList(conditions.Condition<conditions.CategoryAttribute> condition){
		//TODO
	}
	
	public void deleteCategory(pojo.Category category) {
		//TODO using the id
		return;
	}
	
}
