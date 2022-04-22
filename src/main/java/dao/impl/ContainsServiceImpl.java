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
			
			
			//Join datasets or return 
			Dataset<Contains> res = fullOuterJoinsContains(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Territory> lonelyTerritory = null;
			Dataset<Region> lonelyRegion = null;
			
			List<Dataset<Territory>> lonelyterritoryList = new ArrayList<Dataset<Territory>>();
			lonelyterritoryList.add(territoryService.getTerritoryListInEmployeesFromMyMongoDB(territory_condition, new MutableBoolean(false)));
			lonelyTerritory = TerritoryService.fullOuterJoinsTerritory(lonelyterritoryList);
			if(lonelyTerritory != null) {
				res = fullLeftOuterJoinBetweenContainsAndTerritory(res, lonelyTerritory);
			}	
		
			List<Dataset<Region>> lonelyregionList = new ArrayList<Dataset<Region>>();
			lonelyregionList.add(regionService.getRegionListInEmployeesFromMyMongoDB(region_condition, new MutableBoolean(false)));
			lonelyRegion = RegionService.fullOuterJoinsRegion(lonelyregionList);
			if(lonelyRegion != null) {
				res = fullLeftOuterJoinBetweenContainsAndRegion(res, lonelyRegion);
			}	
		
			
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
