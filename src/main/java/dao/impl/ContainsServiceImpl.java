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
import conditions.ContainsAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.TerritoryTDO;
import tdo.ContainsTDO;
import conditions.TerritoryAttribute;
import dao.services.TerritoryService;
import tdo.RegionTDO;
import tdo.ContainsTDO;
import conditions.RegionAttribute;
import dao.services.RegionService;
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

public class ContainsServiceImpl extends dao.services.ContainsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ContainsServiceImpl.class);
	
	// method accessing the embedded object region mapped to role territory
	public Dataset<Contains> getContainsListInmongoSchemaEmployeesterritoriesregion(Condition<TerritoryAttribute> territory_condition, Condition<RegionAttribute> region_condition, MutableBoolean territory_refilter, MutableBoolean region_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = TerritoryServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(territory_condition ,territory_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = RegionServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(region_condition ,region_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
		
			Dataset<Contains> res = dataset.flatMap((FlatMapFunction<Row, Contains>) r -> {
					List<Contains> list_res = new ArrayList<Contains>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Contains contains1 = new Contains();
					contains1.setTerritory(new Territory());
					contains1.setRegion(new Region());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Contains contains2 = (Contains) contains1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territory.description for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									contains2.getTerritory().setDescription(null);
								else{
									contains2.getTerritory().setDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territory.id for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									contains2.getTerritory().setId(null);
								else{
									contains2.getTerritory().setId(Util.getIntegerValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.description for field RegionDescription			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionDescription")) {
								if(nestedRow.getAs("RegionDescription")==null)
									contains2.getRegion().setDescription(null);
								else{
									contains2.getRegion().setDescription(Util.getStringValue(nestedRow.getAs("RegionDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.id for field RegionID			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionID")) {
								if(nestedRow.getAs("RegionID")==null)
									contains2.getRegion().setId(null);
								else{
									contains2.getRegion().setId(Util.getIntegerValue(nestedRow.getAs("RegionID")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((territory_condition == null || territory_refilter.booleanValue() || territory_condition.evaluate(contains2.getTerritory()))&&(region_condition == null || region_refilter.booleanValue() || region_condition.evaluate(contains2.getRegion())))) {
								if(!(contains2.getTerritory().equals(new Territory())) && !(contains2.getRegion().equals(new Region())))
									list_res.add(contains2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(contains1.getTerritory().equals(new Territory())) && !(contains1.getRegion().equals(new Region())))
							list_res.add(contains1);
						addedInList = true;
					} 
					
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Contains contains2 = (Contains) contains1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territory.description for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									contains2.getTerritory().setDescription(null);
								else{
									contains2.getTerritory().setDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territory.id for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									contains2.getTerritory().setId(null);
								else{
									contains2.getTerritory().setId(Util.getIntegerValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.description for field RegionDescription			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionDescription")) {
								if(nestedRow.getAs("RegionDescription")==null)
									contains2.getRegion().setDescription(null);
								else{
									contains2.getRegion().setDescription(Util.getStringValue(nestedRow.getAs("RegionDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.id for field RegionID			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionID")) {
								if(nestedRow.getAs("RegionID")==null)
									contains2.getRegion().setId(null);
								else{
									contains2.getRegion().setId(Util.getIntegerValue(nestedRow.getAs("RegionID")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((territory_condition == null || territory_refilter.booleanValue() || territory_condition.evaluate(contains2.getTerritory()))&&(region_condition == null || region_refilter.booleanValue() || region_condition.evaluate(contains2.getRegion())))) {
								if(!(contains2.getTerritory().equals(new Territory())) && !(contains2.getRegion().equals(new Region())))
									list_res.add(contains2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(contains1.getTerritory().equals(new Territory())) && !(contains1.getRegion().equals(new Region())))
							list_res.add(contains1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Contains.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	public Dataset<Contains> getContainsList(
		Condition<TerritoryAttribute> territory_condition,
		Condition<RegionAttribute> region_condition){
			ContainsServiceImpl containsService = this;
			TerritoryService territoryService = new TerritoryServiceImpl();  
			RegionService regionService = new RegionServiceImpl();
			MutableBoolean territory_refilter = new MutableBoolean(false);
			List<Dataset<Contains>> datasetsPOJO = new ArrayList<Dataset<Contains>>();
			boolean all_already_persisted = false;
			MutableBoolean region_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Contains> res_contains_territory;
			Dataset<Territory> res_Territory;
			// Role 'territory' mapped to EmbeddedObject 'region' - 'Region' containing 'Territory'
			region_refilter = new MutableBoolean(false);
			res_contains_territory = containsService.getContainsListInmongoSchemaEmployeesterritoriesregion(territory_condition, region_condition, territory_refilter, region_refilter);
		 	
			datasetsPOJO.add(res_contains_territory);
			
			
			//Join datasets or return 
			Dataset<Contains> res = fullOuterJoinsContains(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Territory> lonelyTerritory = null;
			Dataset<Region> lonelyRegion = null;
			
		
		
			
			if(territory_refilter.booleanValue() || region_refilter.booleanValue())
				res = res.filter((FilterFunction<Contains>) r -> (territory_condition == null || territory_condition.evaluate(r.getTerritory())) && (region_condition == null || region_condition.evaluate(r.getRegion())));
			
		
			return res;
		
		}
	
	public Dataset<Contains> getContainsListByTerritoryCondition(
		Condition<TerritoryAttribute> territory_condition
	){
		return getContainsList(territory_condition, null);
	}
	
	public Contains getContainsByTerritory(Territory territory) {
		Condition<TerritoryAttribute> cond = null;
		cond = Condition.simple(TerritoryAttribute.id, Operator.EQUALS, territory.getId());
		Dataset<Contains> res = getContainsListByTerritoryCondition(cond);
		List<Contains> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Contains> getContainsListByRegionCondition(
		Condition<RegionAttribute> region_condition
	){
		return getContainsList(null, region_condition);
	}
	
	public Dataset<Contains> getContainsListByRegion(Region region) {
		Condition<RegionAttribute> cond = null;
		cond = Condition.simple(RegionAttribute.id, Operator.EQUALS, region.getId());
		Dataset<Contains> res = getContainsListByRegionCondition(cond);
	return res;
	}
	
	
	
	public void deleteContainsList(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteContainsListByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition
	){
		deleteContainsList(territory_condition, null);
	}
	
	public void deleteContainsByTerritory(pojo.Territory territory) {
		// TODO using id for selecting
		return;
	}
	public void deleteContainsListByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteContainsList(null, region_condition);
	}
	
	public void deleteContainsListByRegion(pojo.Region region) {
		// TODO using id for selecting
		return;
	}
		
}
