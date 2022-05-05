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
import conditions.LocatedInAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.TerritoriesTDO;
import tdo.LocatedInTDO;
import conditions.TerritoriesAttribute;
import dao.services.TerritoriesService;
import tdo.RegionTDO;
import tdo.LocatedInTDO;
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

public class LocatedInServiceImpl extends dao.services.LocatedInService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocatedInServiceImpl.class);
	
	// method accessing the embedded object region mapped to role territories
	public Dataset<LocatedIn> getLocatedInListInmongoDBEmployeesterritoriesregion(Condition<TerritoriesAttribute> territories_condition, Condition<RegionAttribute> region_condition, MutableBoolean territories_refilter, MutableBoolean region_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = TerritoriesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(territories_condition ,territories_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = RegionServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(region_condition ,region_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
		
			Dataset<LocatedIn> res = dataset.flatMap((FlatMapFunction<Row, LocatedIn>) r -> {
					List<LocatedIn> list_res = new ArrayList<LocatedIn>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					LocatedIn locatedIn1 = new LocatedIn();
					locatedIn1.setTerritories(new Territories());
					locatedIn1.setRegion(new Region());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							LocatedIn locatedIn2 = (LocatedIn) locatedIn1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territories.territoryID for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									locatedIn2.getTerritories().setTerritoryID(null);
								else{
									locatedIn2.getTerritories().setTerritoryID(Util.getStringValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territories.territoryDescription for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									locatedIn2.getTerritories().setTerritoryDescription(null);
								else{
									locatedIn2.getTerritories().setTerritoryDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.regionID for field RegionID			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionID")) {
								if(nestedRow.getAs("RegionID")==null)
									locatedIn2.getRegion().setRegionID(null);
								else{
									locatedIn2.getRegion().setRegionID(Util.getIntegerValue(nestedRow.getAs("RegionID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.regionDescription for field RegionDescription			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionDescription")) {
								if(nestedRow.getAs("RegionDescription")==null)
									locatedIn2.getRegion().setRegionDescription(null);
								else{
									locatedIn2.getRegion().setRegionDescription(Util.getStringValue(nestedRow.getAs("RegionDescription")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((territories_condition == null || territories_refilter.booleanValue() || territories_condition.evaluate(locatedIn2.getTerritories()))&&(region_condition == null || region_refilter.booleanValue() || region_condition.evaluate(locatedIn2.getRegion())))) {
								if(!(locatedIn2.getTerritories().equals(new Territories())) && !(locatedIn2.getRegion().equals(new Region())))
									list_res.add(locatedIn2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(locatedIn1.getTerritories().equals(new Territories())) && !(locatedIn1.getRegion().equals(new Region())))
							list_res.add(locatedIn1);
						addedInList = true;
					} 
					
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							LocatedIn locatedIn2 = (LocatedIn) locatedIn1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territories.territoryID for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									locatedIn2.getTerritories().setTerritoryID(null);
								else{
									locatedIn2.getTerritories().setTerritoryID(Util.getStringValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territories.territoryDescription for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									locatedIn2.getTerritories().setTerritoryDescription(null);
								else{
									locatedIn2.getTerritories().setTerritoryDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.regionID for field RegionID			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionID")) {
								if(nestedRow.getAs("RegionID")==null)
									locatedIn2.getRegion().setRegionID(null);
								else{
									locatedIn2.getRegion().setRegionID(Util.getIntegerValue(nestedRow.getAs("RegionID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Region.regionDescription for field RegionDescription			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("region");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RegionDescription")) {
								if(nestedRow.getAs("RegionDescription")==null)
									locatedIn2.getRegion().setRegionDescription(null);
								else{
									locatedIn2.getRegion().setRegionDescription(Util.getStringValue(nestedRow.getAs("RegionDescription")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((territories_condition == null || territories_refilter.booleanValue() || territories_condition.evaluate(locatedIn2.getTerritories()))&&(region_condition == null || region_refilter.booleanValue() || region_condition.evaluate(locatedIn2.getRegion())))) {
								if(!(locatedIn2.getTerritories().equals(new Territories())) && !(locatedIn2.getRegion().equals(new Region())))
									list_res.add(locatedIn2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(locatedIn1.getTerritories().equals(new Territories())) && !(locatedIn1.getRegion().equals(new Region())))
							list_res.add(locatedIn1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(LocatedIn.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	public Dataset<LocatedIn> getLocatedInList(
		Condition<TerritoriesAttribute> territories_condition,
		Condition<RegionAttribute> region_condition){
			LocatedInServiceImpl locatedInService = this;
			TerritoriesService territoriesService = new TerritoriesServiceImpl();  
			RegionService regionService = new RegionServiceImpl();
			MutableBoolean territories_refilter = new MutableBoolean(false);
			List<Dataset<LocatedIn>> datasetsPOJO = new ArrayList<Dataset<LocatedIn>>();
			boolean all_already_persisted = false;
			MutableBoolean region_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<LocatedIn> res_locatedIn_territories;
			Dataset<Territories> res_Territories;
			// Role 'territories' mapped to EmbeddedObject 'region' - 'Region' containing 'Territories'
			region_refilter = new MutableBoolean(false);
			res_locatedIn_territories = locatedInService.getLocatedInListInmongoDBEmployeesterritoriesregion(territories_condition, region_condition, territories_refilter, region_refilter);
		 	
			datasetsPOJO.add(res_locatedIn_territories);
			
			
			//Join datasets or return 
			Dataset<LocatedIn> res = fullOuterJoinsLocatedIn(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Territories> lonelyTerritories = null;
			Dataset<Region> lonelyRegion = null;
			
		
		
			
			if(territories_refilter.booleanValue() || region_refilter.booleanValue())
				res = res.filter((FilterFunction<LocatedIn>) r -> (territories_condition == null || territories_condition.evaluate(r.getTerritories())) && (region_condition == null || region_condition.evaluate(r.getRegion())));
			
		
			return res;
		
		}
	
	public Dataset<LocatedIn> getLocatedInListByTerritoriesCondition(
		Condition<TerritoriesAttribute> territories_condition
	){
		return getLocatedInList(territories_condition, null);
	}
	
	public LocatedIn getLocatedInByTerritories(Territories territories) {
		Condition<TerritoriesAttribute> cond = null;
		cond = Condition.simple(TerritoriesAttribute.territoryID, Operator.EQUALS, territories.getTerritoryID());
		Dataset<LocatedIn> res = getLocatedInListByTerritoriesCondition(cond);
		List<LocatedIn> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<LocatedIn> getLocatedInListByRegionCondition(
		Condition<RegionAttribute> region_condition
	){
		return getLocatedInList(null, region_condition);
	}
	
	public Dataset<LocatedIn> getLocatedInListByRegion(Region region) {
		Condition<RegionAttribute> cond = null;
		cond = Condition.simple(RegionAttribute.regionID, Operator.EQUALS, region.getRegionID());
		Dataset<LocatedIn> res = getLocatedInListByRegionCondition(cond);
	return res;
	}
	
	
	
	public void deleteLocatedInList(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteLocatedInListByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteLocatedInList(territories_condition, null);
	}
	
	public void deleteLocatedInByTerritories(pojo.Territories territories) {
		// TODO using id for selecting
		return;
	}
	public void deleteLocatedInListByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteLocatedInList(null, region_condition);
	}
	
	public void deleteLocatedInListByRegion(pojo.Region region) {
		// TODO using id for selecting
		return;
	}
		
}
