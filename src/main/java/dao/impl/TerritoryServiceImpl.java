package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Territory;
import conditions.*;
import dao.services.TerritoryService;
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


public class TerritoryServiceImpl extends TerritoryService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TerritoryServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInEmployeesFromMyMongoDB(conditions.SetClause<TerritoryAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<TerritoryAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<TerritoryAttribute, Object> e : clause.entrySet()) {
				TerritoryAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == TerritoryAttribute.id ) {
					String fieldName = "TerritoryID";
					fieldName = "territories.$[territories0]." + fieldName;
					arrayFields.add("territories0");
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == TerritoryAttribute.description ) {
					String fieldName = "TerritoryDescription";
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
	
	public static String getBSONMatchQueryInEmployeesFromMyMongoDB(Condition<TerritoryAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				TerritoryAttribute attr = ((SimpleCondition<TerritoryAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<TerritoryAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<TerritoryAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == TerritoryAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "TerritoryID': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "territories." + res;
					res = "'" + res;
					}
					if(attr == TerritoryAttribute.description ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "TerritoryDescription': {" + mongoOp + ": " + preparedValue + "}";
	
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(Condition<TerritoryAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				TerritoryAttribute attr = ((SimpleCondition<TerritoryAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<TerritoryAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<TerritoryAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == TerritoryAttribute.id ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "TerritoryID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
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
					if(attr == TerritoryAttribute.description ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "TerritoryDescription': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
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
	
	
	
	public Dataset<Territory> getTerritoryListInEmployeesFromMyMongoDB(conditions.Condition<conditions.TerritoryAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = TerritoryServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<Territory> res = dataset.flatMap((FlatMapFunction<Row, Territory>) r -> {
				Set<Territory> list_res = new HashSet<Territory>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Territory territory1 = new Territory();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Territory territory2 = (Territory) territory1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territory.description for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									territory2.setDescription(null);
								else{
									territory2.setDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territory.id for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									territory2.setId(null);
								else{
									territory2.setId(Util.getIntegerValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							if(toAdd2&& (condition ==null || refilterFlag.booleanValue() || condition.evaluate(territory2))) {
								list_res.add(territory2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						list_res.add(territory1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Territory.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Territory> getTerritoryListInContains(conditions.Condition<conditions.TerritoryAttribute> territory_condition,conditions.Condition<conditions.RegionAttribute> region_condition)		{
		MutableBoolean territory_refilter = new MutableBoolean(false);
		List<Dataset<Territory>> datasetsPOJO = new ArrayList<Dataset<Territory>>();
		Dataset<Region> all = null;
		boolean all_already_persisted = false;
		MutableBoolean region_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Contains> res_contains_territory;
		Dataset<Territory> res_Territory;
		// Role 'territory' mapped to EmbeddedObject 'region' - 'Region' containing 'Territory'
		region_refilter = new MutableBoolean(false);
		res_contains_territory = containsService.getContainsListInmongoSchemaEmployeesterritoriesregion(territory_condition, region_condition, territory_refilter, region_refilter);
		if(region_refilter.booleanValue()) {
			if(all == null)
				all = new RegionServiceImpl().getRegionList(region_condition);
			joinCondition = null;
			joinCondition = res_contains_territory.col("region.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Territory = res_contains_territory.join(all).select("territory.*").as(Encoders.bean(Territory.class));
			else
				res_Territory = res_contains_territory.join(all, joinCondition).select("territory.*").as(Encoders.bean(Territory.class));
		
		} else
			res_Territory = res_contains_territory.map((MapFunction<Contains,Territory>) r -> r.getTerritory(), Encoders.bean(Territory.class));
		res_Territory = res_Territory.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Territory);
		
		
		//Join datasets or return 
		Dataset<Territory> res = fullOuterJoinsTerritory(datasetsPOJO);
		if(res == null)
			return null;
	
		if(territory_refilter.booleanValue())
			res = res.filter((FilterFunction<Territory>) r -> territory_condition == null || territory_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Territory> getTerritoryListInAre_in(conditions.Condition<conditions.EmployeeAttribute> employee_condition,conditions.Condition<conditions.TerritoryAttribute> territory_condition)		{
		MutableBoolean territory_refilter = new MutableBoolean(false);
		List<Dataset<Territory>> datasetsPOJO = new ArrayList<Dataset<Territory>>();
		Dataset<Employee> all = null;
		boolean all_already_persisted = false;
		MutableBoolean employee_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Are_in> res_are_in_territory;
		Dataset<Territory> res_Territory;
		// Role 'employee' mapped to EmbeddedObject 'territories' 'Territory' containing 'Employee' 
		employee_refilter = new MutableBoolean(false);
		res_are_in_territory = are_inService.getAre_inListInmongoSchemaEmployeesterritories(employee_condition, territory_condition, employee_refilter, territory_refilter);
		if(employee_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeeServiceImpl().getEmployeeList(employee_condition);
			joinCondition = null;
			joinCondition = res_are_in_territory.col("employee.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Territory = res_are_in_territory.join(all).select("territory.*").as(Encoders.bean(Territory.class));
			else
				res_Territory = res_are_in_territory.join(all, joinCondition).select("territory.*").as(Encoders.bean(Territory.class));
		
		} else
			res_Territory = res_are_in_territory.map((MapFunction<Are_in,Territory>) r -> r.getTerritory(), Encoders.bean(Territory.class));
		res_Territory = res_Territory.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Territory);
		
		
		//Join datasets or return 
		Dataset<Territory> res = fullOuterJoinsTerritory(datasetsPOJO);
		if(res == null)
			return null;
	
		if(territory_refilter.booleanValue())
			res = res.filter((FilterFunction<Territory>) r -> territory_condition == null || territory_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertTerritory(
		Territory territory,
		Region	regionContains){
			boolean inserted = false;
			// Insert in standalone structures
			// Insert in structures containing double embedded role
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			// Insert in ref structures mapped to opposite role of mandatory role  
			return inserted;
		}
	
	private boolean inUpdateMethod = false;
	private List<Row> allTerritoryIdList = null;
	public void updateTerritoryList(conditions.Condition<conditions.TerritoryAttribute> condition, conditions.SetClause<conditions.TerritoryAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateTerritory(pojo.Territory territory) {
		//TODO using the id
		return;
	}
	public void updateTerritoryListInContains(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.TerritoryAttribute> set
	){
		//TODO
	}
	
	public void updateTerritoryListInContainsByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.SetClause<conditions.TerritoryAttribute> set
	){
		updateTerritoryListInContains(territory_condition, null, set);
	}
	public void updateTerritoryListInContainsByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition,
		conditions.SetClause<conditions.TerritoryAttribute> set
	){
		updateTerritoryListInContains(null, region_condition, set);
	}
	
	public void updateTerritoryListInContainsByRegion(
		pojo.Region region,
		conditions.SetClause<conditions.TerritoryAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateTerritoryListInAre_in(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		
		conditions.SetClause<conditions.TerritoryAttribute> set
	){
		//TODO
	}
	
	public void updateTerritoryListInAre_inByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.SetClause<conditions.TerritoryAttribute> set
	){
		updateTerritoryListInAre_in(employee_condition, null, set);
	}
	
	public void updateTerritoryListInAre_inByEmployee(
		pojo.Employee employee,
		conditions.SetClause<conditions.TerritoryAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateTerritoryListInAre_inByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.SetClause<conditions.TerritoryAttribute> set
	){
		updateTerritoryListInAre_in(null, territory_condition, set);
	}
	
	
	public void deleteTerritoryList(conditions.Condition<conditions.TerritoryAttribute> condition){
		//TODO
	}
	
	public void deleteTerritory(pojo.Territory territory) {
		//TODO using the id
		return;
	}
	public void deleteTerritoryListInContains(	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteTerritoryListInContainsByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition
	){
		deleteTerritoryListInContains(territory_condition, null);
	}
	public void deleteTerritoryListInContainsByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteTerritoryListInContains(null, region_condition);
	}
	
	public void deleteTerritoryListInContainsByRegion(
		pojo.Region region 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteTerritoryListInAre_in(	
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition){
			//TODO
		}
	
	public void deleteTerritoryListInAre_inByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteTerritoryListInAre_in(employee_condition, null);
	}
	
	public void deleteTerritoryListInAre_inByEmployee(
		pojo.Employee employee 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteTerritoryListInAre_inByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition
	){
		deleteTerritoryListInAre_in(null, territory_condition);
	}
	
}
