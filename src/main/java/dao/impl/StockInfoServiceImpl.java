package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.StockInfo;
import conditions.*;
import dao.services.StockInfoService;
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


public class StockInfoServiceImpl extends StockInfoService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StockInfoServiceImpl.class);
	
	
	
	
	
	
	
	//TODO redis
	public Dataset<StockInfo> getStockInfoListInStockInfoPairsFromRedisDB(conditions.Condition<conditions.StockInfoAttribute> condition, MutableBoolean refilterFlag){
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
		Dataset<StockInfo> res = rows.map((MapFunction<Row, StockInfo>) r -> {
					StockInfo stockInfo_res = new StockInfo();
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
	
						return stockInfo_res;
				}, Encoders.bean(StockInfo.class));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<StockInfo> getStockListInConcern(conditions.Condition<conditions.StockInfoAttribute> stock_condition,conditions.Condition<conditions.ProductInfoAttribute> product_condition)		{
		MutableBoolean stock_refilter = new MutableBoolean(false);
		List<Dataset<StockInfo>> datasetsPOJO = new ArrayList<Dataset<StockInfo>>();
		Dataset<ProductInfo> all = null;
		boolean all_already_persisted = false;
		MutableBoolean product_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'stock' in reference 'concerned'. A->B Scenario
		product_refilter = new MutableBoolean(false);
		Dataset<StockInfoTDO> stockInfoTDOconcernedstock = concernService.getStockInfoTDOListStockInConcernedInStockInfoPairsFromKv(stock_condition, stock_refilter);
		Dataset<ProductInfoTDO> productInfoTDOconcernedproduct = concernService.getProductInfoTDOListProductInConcernedInStockInfoPairsFromKv(product_condition, product_refilter);
		if(product_refilter.booleanValue()) {
			if(all == null)
				all = new ProductInfoServiceImpl().getProductInfoList(product_condition);
			joinCondition = null;
			joinCondition = productInfoTDOconcernedproduct.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				productInfoTDOconcernedproduct = productInfoTDOconcernedproduct.as("A").join(all).select("A.*").as(Encoders.bean(ProductInfoTDO.class));
			else
				productInfoTDOconcernedproduct = productInfoTDOconcernedproduct.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductInfoTDO.class));
		}
	
		
		Dataset<Row> res_concerned = stockInfoTDOconcernedstock.join(productInfoTDOconcernedproduct
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
		Dataset<StockInfo> res_StockInfo_concerned = res_concerned.select( "id", "unitsInStock", "unitsOnOrder", "logEvents").as(Encoders.bean(StockInfo.class));
		
		res_StockInfo_concerned = res_StockInfo_concerned.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_StockInfo_concerned);
		
		
		Dataset<Concern> res_concern_stock;
		Dataset<StockInfo> res_StockInfo;
		
		
		//Join datasets or return 
		Dataset<StockInfo> res = fullOuterJoinsStockInfo(datasetsPOJO);
		if(res == null)
			return null;
	
		if(stock_refilter.booleanValue())
			res = res.filter((FilterFunction<StockInfo>) r -> stock_condition == null || stock_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertStockInfo(StockInfo stockInfo){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertStockInfoInStockInfoPairsFromRedisDB(stockInfo) || inserted ;
		return inserted;
	}
	
	public boolean insertStockInfoInStockInfoPairsFromRedisDB(StockInfo stockInfo)	{
		String idvalue="";
		idvalue+=stockInfo.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "PRODUCT:";
			key += stockInfo.getId();
			key += ":STOCKINFO";
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_UnitsInStock="UnitsInStock";
			String _value_UnitsInStock="";
			if(stockInfo.getUnitsInStock()!=null){
				toAdd = true;
				_value_UnitsInStock += stockInfo.getUnitsInStock();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsInStock,_value_UnitsInStock));
			toAdd = false;
			String _fieldname_UnitsOnOrder="UnitsOnOrder";
			String _value_UnitsOnOrder="";
			if(stockInfo.getUnitsOnOrder()!=null){
				toAdd = true;
				_value_UnitsOnOrder += stockInfo.getUnitsOnOrder();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsOnOrder,_value_UnitsOnOrder));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "redisDB");
	
			logger.info("Inserted [StockInfo] entity ID [{}] in [StockInfoPairs] in database [RedisDB]", idvalue);
		}
		else
			logger.warn("[StockInfo] entity ID [{}] already present in [StockInfoPairs] in database [RedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allStockInfoIdList = null;
	public void updateStockInfoList(conditions.Condition<conditions.StockInfoAttribute> condition, conditions.SetClause<conditions.StockInfoAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInStockInfoPairsFromRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInStockInfoPairsFromRedisDB.booleanValue())
				updateStockInfoListInStockInfoPairsFromRedisDB(condition, set);
		
	
			if(!refilterInStockInfoPairsFromRedisDB.booleanValue())
				updateStockInfoListInStockInfoPairsFromRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateStockInfoListInStockInfoPairsFromRedisDB(Condition<StockInfoAttribute> condition, SetClause<StockInfoAttribute> set) {
		//TODO
	}
	
	
	
	public void updateStockInfo(pojo.StockInfo stockinfo) {
		//TODO using the id
		return;
	}
	public void updateStockListInConcern(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		
		conditions.SetClause<conditions.StockInfoAttribute> set
	){
		//TODO
	}
	
	public void updateStockListInConcernByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.SetClause<conditions.StockInfoAttribute> set
	){
		updateStockListInConcern(stock_condition, null, set);
	}
	public void updateStockListInConcernByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition,
		conditions.SetClause<conditions.StockInfoAttribute> set
	){
		updateStockListInConcern(null, product_condition, set);
	}
	
	public void updateStockInConcernByProduct(
		pojo.ProductInfo product,
		conditions.SetClause<conditions.StockInfoAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteStockInfoList(conditions.Condition<conditions.StockInfoAttribute> condition){
		//TODO
	}
	
	public void deleteStockInfo(pojo.StockInfo stockinfo) {
		//TODO using the id
		return;
	}
	public void deleteStockListInConcern(	
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,	
		conditions.Condition<conditions.ProductInfoAttribute> product_condition){
			//TODO
		}
	
	public void deleteStockListInConcernByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition
	){
		deleteStockListInConcern(stock_condition, null);
	}
	public void deleteStockListInConcernByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition
	){
		deleteStockListInConcern(null, product_condition);
	}
	
	public void deleteStockInConcernByProduct(
		pojo.ProductInfo product 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
