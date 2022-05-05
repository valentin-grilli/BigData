package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Territories;
import conditions.*;
import dao.services.TerritoriesService;
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


public class TerritoriesServiceImpl extends TerritoriesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TerritoriesServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInEmployeesFromMyMongoDB(conditions.SetClause<TerritoriesAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<TerritoriesAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<TerritoriesAttribute, Object> e : clause.entrySet()) {
				TerritoriesAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == TerritoriesAttribute.territoryID ) {
					String fieldName = "TerritoryID";
					fieldName = "territories.$[territories0]." + fieldName;
					arrayFields.add("territories0");
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == TerritoriesAttribute.territoryDescription ) {
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
	
	public static String getBSONMatchQueryInEmployeesFromMyMongoDB(Condition<TerritoriesAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				TerritoriesAttribute attr = ((SimpleCondition<TerritoriesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<TerritoriesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<TerritoriesAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == TerritoriesAttribute.territoryID ) {
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
					if(attr == TerritoriesAttribute.territoryDescription ) {
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(Condition<TerritoriesAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				TerritoriesAttribute attr = ((SimpleCondition<TerritoriesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<TerritoriesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<TerritoriesAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == TerritoriesAttribute.territoryID ) {
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
					if(attr == TerritoriesAttribute.territoryDescription ) {
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
	
	
	
	public Dataset<Territories> getTerritoriesListInEmployeesFromMyMongoDB(conditions.Condition<conditions.TerritoriesAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = TerritoriesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<Territories> res = dataset.flatMap((FlatMapFunction<Row, Territories>) r -> {
				Set<Territories> list_res = new HashSet<Territories>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Territories territories1 = new Territories();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Territories territories2 = (Territories) territories1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territories.territoryID for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									territories2.setTerritoryID(null);
								else{
									territories2.setTerritoryID(Util.getStringValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territories.territoryDescription for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									territories2.setTerritoryDescription(null);
								else{
									territories2.setTerritoryDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							if(toAdd2&& (condition ==null || refilterFlag.booleanValue() || condition.evaluate(territories2))) {
								list_res.add(territories2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						list_res.add(territories1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Territories.class));
		res= res.dropDuplicates(new String[]{"territoryID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Territories> getTerritoriesListInLocatedIn(conditions.Condition<conditions.TerritoriesAttribute> territories_condition,conditions.Condition<conditions.RegionAttribute> region_condition)		{
		MutableBoolean territories_refilter = new MutableBoolean(false);
		List<Dataset<Territories>> datasetsPOJO = new ArrayList<Dataset<Territories>>();
		Dataset<Region> all = null;
		boolean all_already_persisted = false;
		MutableBoolean region_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<LocatedIn> res_locatedIn_territories;
		Dataset<Territories> res_Territories;
		// Role 'territories' mapped to EmbeddedObject 'region' - 'Region' containing 'Territories'
		region_refilter = new MutableBoolean(false);
		res_locatedIn_territories = locatedInService.getLocatedInListInmongoDBEmployeesterritoriesregion(territories_condition, region_condition, territories_refilter, region_refilter);
		if(region_refilter.booleanValue()) {
			if(all == null)
				all = new RegionServiceImpl().getRegionList(region_condition);
			joinCondition = null;
			joinCondition = res_locatedIn_territories.col("region.regionID").equalTo(all.col("regionID"));
			if(joinCondition == null)
				res_Territories = res_locatedIn_territories.join(all).select("territories.*").as(Encoders.bean(Territories.class));
			else
				res_Territories = res_locatedIn_territories.join(all, joinCondition).select("territories.*").as(Encoders.bean(Territories.class));
		
		} else
			res_Territories = res_locatedIn_territories.map((MapFunction<LocatedIn,Territories>) r -> r.getTerritories(), Encoders.bean(Territories.class));
		res_Territories = res_Territories.dropDuplicates(new String[] {"territoryID"});
		datasetsPOJO.add(res_Territories);
		
		
		//Join datasets or return 
		Dataset<Territories> res = fullOuterJoinsTerritories(datasetsPOJO);
		if(res == null)
			return null;
	
		if(territories_refilter.booleanValue())
			res = res.filter((FilterFunction<Territories>) r -> territories_condition == null || territories_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Territories> getTerritoriesListInWorks(conditions.Condition<conditions.EmployeesAttribute> employed_condition,conditions.Condition<conditions.TerritoriesAttribute> territories_condition)		{
		MutableBoolean territories_refilter = new MutableBoolean(false);
		List<Dataset<Territories>> datasetsPOJO = new ArrayList<Dataset<Territories>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean employed_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Works> res_works_territories;
		Dataset<Territories> res_Territories;
		// Role 'employed' mapped to EmbeddedObject 'territories' 'Territories' containing 'Employees' 
		employed_refilter = new MutableBoolean(false);
		res_works_territories = worksService.getWorksListInmongoDBEmployeesterritories(employed_condition, territories_condition, employed_refilter, territories_refilter);
		if(employed_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeesServiceImpl().getEmployeesList(employed_condition);
			joinCondition = null;
			joinCondition = res_works_territories.col("employed.employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				res_Territories = res_works_territories.join(all).select("territories.*").as(Encoders.bean(Territories.class));
			else
				res_Territories = res_works_territories.join(all, joinCondition).select("territories.*").as(Encoders.bean(Territories.class));
		
		} else
			res_Territories = res_works_territories.map((MapFunction<Works,Territories>) r -> r.getTerritories(), Encoders.bean(Territories.class));
		res_Territories = res_Territories.dropDuplicates(new String[] {"territoryID"});
		datasetsPOJO.add(res_Territories);
		
		
		//Join datasets or return 
		Dataset<Territories> res = fullOuterJoinsTerritories(datasetsPOJO);
		if(res == null)
			return null;
	
		if(territories_refilter.booleanValue())
			res = res.filter((FilterFunction<Territories>) r -> territories_condition == null || territories_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertTerritories(
		Territories territories,
		Region	regionLocatedIn){
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
	private List<Row> allTerritoriesIdList = null;
	public void updateTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition, conditions.SetClause<conditions.TerritoriesAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateTerritories(pojo.Territories territories) {
		//TODO using the id
		return;
	}
	public void updateTerritoriesListInLocatedIn(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		//TODO
	}
	
	public void updateTerritoriesListInLocatedInByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInLocatedIn(territories_condition, null, set);
	}
	public void updateTerritoriesListInLocatedInByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInLocatedIn(null, region_condition, set);
	}
	
	public void updateTerritoriesListInLocatedInByRegion(
		pojo.Region region,
		conditions.SetClause<conditions.TerritoriesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateTerritoriesListInWorks(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		//TODO
	}
	
	public void updateTerritoriesListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInWorks(employed_condition, null, set);
	}
	
	public void updateTerritoriesListInWorksByEmployed(
		pojo.Employees employed,
		conditions.SetClause<conditions.TerritoriesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateTerritoriesListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInWorks(null, territories_condition, set);
	}
	
	
	public void deleteTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition){
		//TODO
	}
	
	public void deleteTerritories(pojo.Territories territories) {
		//TODO using the id
		return;
	}
	public void deleteTerritoriesListInLocatedIn(	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteTerritoriesListInLocatedInByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteTerritoriesListInLocatedIn(territories_condition, null);
	}
	public void deleteTerritoriesListInLocatedInByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteTerritoriesListInLocatedIn(null, region_condition);
	}
	
	public void deleteTerritoriesListInLocatedInByRegion(
		pojo.Region region 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteTerritoriesListInWorks(	
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
			//TODO
		}
	
	public void deleteTerritoriesListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteTerritoriesListInWorks(employed_condition, null);
	}
	
	public void deleteTerritoriesListInWorksByEmployed(
		pojo.Employees employed 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteTerritoriesListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteTerritoriesListInWorks(null, territories_condition);
	}
	
}
