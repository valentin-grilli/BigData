package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Region;
import conditions.*;
import dao.services.RegionService;
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


public class RegionServiceImpl extends RegionService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegionServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInEmployeesFromMyMongoDB(conditions.SetClause<RegionAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<RegionAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<RegionAttribute, Object> e : clause.entrySet()) {
				RegionAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == RegionAttribute.id ) {
					String fieldName = "RegionID";
					fieldName = "region." + fieldName;
					fieldName = "territories.$[territories0]." + fieldName;
					arrayFields.add("territories0");
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == RegionAttribute.description ) {
					String fieldName = "RegionDescription";
					fieldName = "region." + fieldName;
					fieldName = "territories.$[territories0]." + fieldName;
					arrayFields.add("territories0");
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return new ImmutablePair<List<String>, List<String>>(res, new ArrayList<String>(arrayFields));
	}
	
	public static String getBSONMatchQueryInEmployeesFromMyMongoDB(Condition<RegionAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				RegionAttribute attr = ((SimpleCondition<RegionAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<RegionAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<RegionAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == RegionAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "RegionID': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "region." + res;
						res = "territories." + res;
					res = "'" + res;
					}
					if(attr == RegionAttribute.description ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "RegionDescription': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "region." + res;
						res = "territories." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInEmployeesFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInEmployeesFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(Condition<RegionAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				RegionAttribute attr = ((SimpleCondition<RegionAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<RegionAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<RegionAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == RegionAttribute.id ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "RegionID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "region." + bson;
						}
						if(!arrayVar) {
							if(arrayVariableNames.contains("territories0")) {
								arrayVar = true;
								arrayVariablesUsed.add("territories0");
								bson = "territories0." + bson; 
							} else {
								bson = "territories." + bson;
							}
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == RegionAttribute.description ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "RegionDescription': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "region." + bson;
						}
						if(!arrayVar) {
							if(arrayVariableNames.contains("territories0")) {
								arrayVar = true;
								arrayVariablesUsed.add("territories0");
								bson = "territories0." + bson; 
							} else {
								bson = "territories." + bson;
							}
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
					}
					
				}
	
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $and: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight);
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $or: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight); // can be a problem
			}
		}
	
		return new ImmutablePair<String, List<String>>(query, arrayFilters);
	}
	
	
	
	public Dataset<Region> getRegionListInEmployeesFromMyMongoDB(conditions.Condition<conditions.RegionAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = RegionServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<Region> res = dataset.flatMap((FlatMapFunction<Row, Region>) r -> {
				Set<Region> list_res = new HashSet<Region>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Region region1 = new Region();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Region region2 = (Region) region1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Region.description for field RegionDescription			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionDescription")) {
								if(nestedRow.getAs("RegionDescription")==null)
									region2.setDescription(null);
								else{
									region2.setDescription(Util.getStringValue(nestedRow.getAs("RegionDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.id for field RegionID			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionID")) {
								if(nestedRow.getAs("RegionID")==null)
									region2.setId(null);
								else{
									region2.setId(Util.getIntegerValue(nestedRow.getAs("RegionID")));
									toAdd2 = true;					
									}
							}
							if(toAdd2&& (condition ==null || refilterFlag.booleanValue() || condition.evaluate(region2))) {
								list_res.add(region2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						list_res.add(region1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Region.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Region> getRegionListInContains(conditions.Condition<conditions.TerritoryAttribute> territory_condition,conditions.Condition<conditions.RegionAttribute> region_condition)		{
		MutableBoolean region_refilter = new MutableBoolean(false);
		List<Dataset<Region>> datasetsPOJO = new ArrayList<Dataset<Region>>();
		Dataset<Territory> all = null;
		boolean all_already_persisted = false;
		MutableBoolean territory_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Contains> res_contains_region;
		Dataset<Region> res_Region;
		
		
		//Join datasets or return 
		Dataset<Region> res = fullOuterJoinsRegion(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Region>> lonelyRegionList = new ArrayList<Dataset<Region>>();
		lonelyRegionList.add(getRegionListInEmployeesFromMyMongoDB(region_condition, new MutableBoolean(false)));
		Dataset<Region> lonelyRegion = fullOuterJoinsRegion(lonelyRegionList);
		if(lonelyRegion != null) {
			res = fullLeftOuterJoinsRegion(Arrays.asList(res, lonelyRegion));
		}
		if(region_refilter.booleanValue())
			res = res.filter((FilterFunction<Region>) r -> region_condition == null || region_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertRegion(
		Region region,
		 List<Territory> territoryContains){
		 	boolean inserted = false;
		 	// Insert in standalone structures
		 	inserted = insertRegionInEmployeesFromMyMongoDB(region)|| inserted ;
		 	// Insert in structures containing double embedded role
		 	// Insert in descending structures
		 	// Insert in ascending structures 
		 	// Insert in ref structures 
		 	// Insert in ref structures mapped to opposite role of mandatory role  
		 	return inserted;
		 }
	
	public boolean insertRegionInEmployeesFromMyMongoDB(Region region)	{
		String idvalue="";
		idvalue+=region.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		Bson filter = new Document();
		Bson updateOp;
		Document docEmployees_1 = new Document();
		// Embedded structure territories
			Document docterritories_2 = new Document();
			// Embedded structure region
				Document docregion_3 = new Document();
				docregion_3.append("RegionDescription",region.getDescription());
				docregion_3.append("RegionID",region.getId());
				
				docterritories_2.append("region", docregion_3);
			
			List<Document> arrayterritories_1 = new ArrayList();
			arrayterritories_1.add(docterritories_2);
			docEmployees_1.append("territories", arrayterritories_1);
		
		filter = eq("RegionID",region.getId());
		updateOp = setOnInsert(docEmployees_1);
		DBConnectionMgr.upsertMany(filter, updateOp, "Employees", "myMongoDB");
			logger.info("Inserted [Region] entity ID [{}] in [Employees] in database [MyMongoDB]", idvalue);
		}
		else
			logger.warn("[Region] entity ID [{}] already present in [Employees] in database [MyMongoDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allRegionIdList = null;
	public void updateRegionList(conditions.Condition<conditions.RegionAttribute> condition, conditions.SetClause<conditions.RegionAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInEmployeesFromMyMongoDB = new MutableBoolean(false);
			getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(condition, new ArrayList<String>(), new HashSet<String>(), refilterInEmployeesFromMyMongoDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInEmployeesFromMyMongoDB.booleanValue())
				updateRegionListInEmployeesFromMyMongoDB(condition, set);
		
	
			if(!refilterInEmployeesFromMyMongoDB.booleanValue())
				updateRegionListInEmployeesFromMyMongoDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateRegionListInEmployeesFromMyMongoDB(Condition<RegionAttribute> condition, SetClause<RegionAttribute> set) {
		Pair<List<String>, List<String>> updates = getBSONUpdateQueryInEmployeesFromMyMongoDB(set);
		List<String> sets = updates.getLeft();
		final List<String> arrayVariableNames = updates.getRight();
		String setBSON = null;
		for(int i = 0; i < sets.size(); i++) {
			if(i == 0)
				setBSON = sets.get(i);
			else
				setBSON += ", " + sets.get(i);
		}
		
		if(setBSON == null)
			return;
		
		Document updateQuery = null;
		setBSON = "{$set: {" + setBSON + "}}";
		updateQuery = Document.parse(setBSON);
		
		MutableBoolean refilter = new MutableBoolean(false);
		Set<String> arrayVariablesUsed = new HashSet<String>();
		Pair<String, List<String>> queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(condition, arrayVariableNames, arrayVariablesUsed, refilter);
		Document query = null;
		String bsonQuery = queryAndArrayFilter.getLeft();
		if(bsonQuery != null) {
			bsonQuery = "{" + bsonQuery + "}";
			query = Document.parse(bsonQuery);	
		}
		
		List<Bson> arrayFilterDocs = new ArrayList<Bson>();
		List<String> arrayFilters = queryAndArrayFilter.getRight();
		for(String arrayFilter : arrayFilters)
			arrayFilterDocs.add(Document.parse( "{" + arrayFilter + "}"));
		
		for(String arrayVariableName : arrayVariableNames)
			if(!arrayVariablesUsed.contains(arrayVariableName)) {
				arrayFilterDocs.add(Document.parse("{" + arrayVariableName + ": {$exists: true}}"));
			}
		
		
		if(!refilter.booleanValue()) {
			if(arrayFilterDocs.size() == 0) {
				DBConnectionMgr.update(query, updateQuery, "Employees", "myMongoDB");
			} else {
				DBConnectionMgr.upsertMany(query, updateQuery, arrayFilterDocs, "Employees", "myMongoDB");
			}
		
			
		} else {
			if(!inUpdateMethod || allRegionIdList == null)
				allRegionIdList = this.getRegionList(condition).select("id").collectAsList();
			List<com.mongodb.client.model.UpdateManyModel<Document>> updateQueries = new ArrayList<com.mongodb.client.model.UpdateManyModel<Document>>();
			for(Row row : allRegionIdList) {
				Condition<RegionAttribute> conditionId = null;
				conditionId = Condition.simple(RegionAttribute.id, Operator.EQUALS, row.getAs("id"));
		
				arrayVariablesUsed = new HashSet<String>();
				queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(conditionId, arrayVariableNames, arrayVariablesUsed, refilter);
				query = null;
				bsonQuery = queryAndArrayFilter.getLeft();
				if(bsonQuery != null) {
					bsonQuery = "{" + bsonQuery + "}";
					query = Document.parse(bsonQuery);	
				}
				
				arrayFilterDocs = new ArrayList<Bson>();
				arrayFilters = queryAndArrayFilter.getRight();
				for(String arrayFilter : arrayFilters)
					arrayFilterDocs.add(Document.parse( "{" + arrayFilter + "}"));
				
				for(String arrayVariableName : arrayVariableNames)
					if(!arrayVariablesUsed.contains(arrayVariableName)) {
						arrayFilterDocs.add(Document.parse("{" + arrayVariableName + ": {$exists: true}}"));
					}
				if(arrayFilterDocs.size() == 0)
					updateQueries.add(new com.mongodb.client.model.UpdateManyModel<Document>(query, updateQuery));
				else
					updateQueries.add(new com.mongodb.client.model.UpdateManyModel<Document>(query, updateQuery, new com.mongodb.client.model.UpdateOptions().arrayFilters(arrayFilterDocs)));
			}
		
			DBConnectionMgr.bulkUpdatesInMongoDB(updateQueries, "Employees", "myMongoDB");
		}
	}
	
	
	
	public void updateRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public void updateRegionListInContains(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.RegionAttribute> set
	){
		//TODO
	}
	
	public void updateRegionListInContainsByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.SetClause<conditions.RegionAttribute> set
	){
		updateRegionListInContains(territory_condition, null, set);
	}
	
	public void updateRegionInContainsByTerritory(
		pojo.Territory territory,
		conditions.SetClause<conditions.RegionAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateRegionListInContainsByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition,
		conditions.SetClause<conditions.RegionAttribute> set
	){
		updateRegionListInContains(null, region_condition, set);
	}
	
	
	public void deleteRegionList(conditions.Condition<conditions.RegionAttribute> condition){
		//TODO
	}
	
	public void deleteRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public void deleteRegionListInContains(	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteRegionListInContainsByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition
	){
		deleteRegionListInContains(territory_condition, null);
	}
	
	public void deleteRegionInContainsByTerritory(
		pojo.Territory territory 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteRegionListInContainsByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteRegionListInContains(null, region_condition);
	}
	
}
