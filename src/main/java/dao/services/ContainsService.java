package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Contains;
import java.time.LocalDate;
import java.time.LocalDateTime;
import tdo.*;
import pojo.*;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class ContainsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ContainsService.class);
	
	// method accessing the embedded object region mapped to role territory
	public abstract Dataset<Contains> getContainsListInmongoSchemaEmployeesterritoriesregion(Condition<TerritoryAttribute> territory_condition, Condition<RegionAttribute> region_condition, MutableBoolean territory_refilter, MutableBoolean region_refilter);
	
	
	public static Dataset<Contains> fullLeftOuterJoinBetweenContainsAndTerritory(Dataset<Contains> d1, Dataset<Territory> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("description", "A_description")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("territory.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Contains>) r -> {
				Contains res = new Contains();
	
				Territory territory = new Territory();
				Object o = r.getAs("territory");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						territory.setId(Util.getIntegerValue(r2.getAs("id")));
						territory.setDescription(Util.getStringValue(r2.getAs("description")));
					} 
					if(o instanceof Territory) {
						territory = (Territory) o;
					}
				}
	
				res.setTerritory(territory);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (territory.getId() != null && id != null && !territory.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Contains - different values found for attribute 'Contains.id': " + territory.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Contains - different values found for attribute 'Contains.id': " + territory.getId() + " and " + id + "." );
				}
				if(id != null)
					territory.setId(id);
				String description = Util.getStringValue(r.getAs("A_description"));
				if (territory.getDescription() != null && description != null && !territory.getDescription().equals(description)) {
					res.addLogEvent("Data consistency problem for [Contains - different values found for attribute 'Contains.description': " + territory.getDescription() + " and " + description + "." );
					logger.warn("Data consistency problem for [Contains - different values found for attribute 'Contains.description': " + territory.getDescription() + " and " + description + "." );
				}
				if(description != null)
					territory.setDescription(description);
	
				o = r.getAs("region");
				Region region = new Region();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						region.setId(Util.getIntegerValue(r2.getAs("id")));
						region.setDescription(Util.getStringValue(r2.getAs("description")));
					} 
					if(o instanceof Region) {
						region = (Region) o;
					}
				}
	
				res.setRegion(region);
	
				return res;
		}, Encoders.bean(Contains.class));
	
		
		
	}
	public static Dataset<Contains> fullLeftOuterJoinBetweenContainsAndRegion(Dataset<Contains> d1, Dataset<Region> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("description", "A_description")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("region.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Contains>) r -> {
				Contains res = new Contains();
	
				Region region = new Region();
				Object o = r.getAs("region");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						region.setId(Util.getIntegerValue(r2.getAs("id")));
						region.setDescription(Util.getStringValue(r2.getAs("description")));
					} 
					if(o instanceof Region) {
						region = (Region) o;
					}
				}
	
				res.setRegion(region);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (region.getId() != null && id != null && !region.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Contains - different values found for attribute 'Contains.id': " + region.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Contains - different values found for attribute 'Contains.id': " + region.getId() + " and " + id + "." );
				}
				if(id != null)
					region.setId(id);
				String description = Util.getStringValue(r.getAs("A_description"));
				if (region.getDescription() != null && description != null && !region.getDescription().equals(description)) {
					res.addLogEvent("Data consistency problem for [Contains - different values found for attribute 'Contains.description': " + region.getDescription() + " and " + description + "." );
					logger.warn("Data consistency problem for [Contains - different values found for attribute 'Contains.description': " + region.getDescription() + " and " + description + "." );
				}
				if(description != null)
					region.setDescription(description);
	
				o = r.getAs("territory");
				Territory territory = new Territory();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						territory.setId(Util.getIntegerValue(r2.getAs("id")));
						territory.setDescription(Util.getStringValue(r2.getAs("description")));
					} 
					if(o instanceof Territory) {
						territory = (Territory) o;
					}
				}
	
				res.setTerritory(territory);
	
				return res;
		}, Encoders.bean(Contains.class));
	
		
		
	}
	
	public static Dataset<Contains> fullOuterJoinsContains(List<Dataset<Contains>> datasetsPOJO) {
		return fullOuterJoinsContains(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Contains> fullLeftOuterJoinsContains(List<Dataset<Contains>> datasetsPOJO) {
		return fullOuterJoinsContains(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Contains> fullOuterJoinsContains(List<Dataset<Contains>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("territory.id");
	
		idFields.add("region.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Contains> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("territory_id_" + i, d.col("territory.id"))
				.withColumn("region_id_" + i, d.col("region.id"))
				.withColumnRenamed("territory", "territory_" + i)
				.withColumnRenamed("region", "region_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("territory_id_0").equalTo(rows.get(1).col("territory_id_1"));
		joinCond = joinCond.and(rows.get(0).col("region_id_0").equalTo(rows.get(1).col("region_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("territory_id_" + (i - 1)).equalTo(rows.get(i).col("territory_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("region_id_" + (i - 1)).equalTo(rows.get(i).col("region_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Contains>) r -> {
				Contains contains_res = new Contains();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							contains_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							contains_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Territory territory_res = new Territory();
					Region region_res = new Region();
					
					// attribute 'Territory.id'
					Integer firstNotNull_territory_id = Util.getIntegerValue(r.getAs("territory_0.id"));
					territory_res.setId(firstNotNull_territory_id);
					// attribute 'Territory.description'
					String firstNotNull_territory_description = Util.getStringValue(r.getAs("territory_0.description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String territory_description2 = Util.getStringValue(r.getAs("territory_" + i + ".description"));
						if (firstNotNull_territory_description != null && territory_description2 != null && !firstNotNull_territory_description.equals(territory_description2)) {
							contains_res.addLogEvent("Data consistency problem for [Territory - id :"+territory_res.getId()+"]: different values found for attribute 'Territory.description': " + firstNotNull_territory_description + " and " + territory_description2 + "." );
							logger.warn("Data consistency problem for [Territory - id :"+territory_res.getId()+"]: different values found for attribute 'Territory.description': " + firstNotNull_territory_description + " and " + territory_description2 + "." );
						}
						if (firstNotNull_territory_description == null && territory_description2 != null) {
							firstNotNull_territory_description = territory_description2;
						}
					}
					territory_res.setDescription(firstNotNull_territory_description);
					// attribute 'Region.id'
					Integer firstNotNull_region_id = Util.getIntegerValue(r.getAs("region_0.id"));
					region_res.setId(firstNotNull_region_id);
					// attribute 'Region.description'
					String firstNotNull_region_description = Util.getStringValue(r.getAs("region_0.description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region_description2 = Util.getStringValue(r.getAs("region_" + i + ".description"));
						if (firstNotNull_region_description != null && region_description2 != null && !firstNotNull_region_description.equals(region_description2)) {
							contains_res.addLogEvent("Data consistency problem for [Region - id :"+region_res.getId()+"]: different values found for attribute 'Region.description': " + firstNotNull_region_description + " and " + region_description2 + "." );
							logger.warn("Data consistency problem for [Region - id :"+region_res.getId()+"]: different values found for attribute 'Region.description': " + firstNotNull_region_description + " and " + region_description2 + "." );
						}
						if (firstNotNull_region_description == null && region_description2 != null) {
							firstNotNull_region_description = region_description2;
						}
					}
					region_res.setDescription(firstNotNull_region_description);
	
					contains_res.setTerritory(territory_res);
					contains_res.setRegion(region_res);
					return contains_res;
		}
		, Encoders.bean(Contains.class));
	
	}
	
	//Empty arguments
	public Dataset<Contains> getContainsList(){
		 return getContainsList(null,null);
	}
	
	public abstract Dataset<Contains> getContainsList(
		Condition<TerritoryAttribute> territory_condition,
		Condition<RegionAttribute> region_condition);
	
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
	
	
	
	public abstract void deleteContainsList(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition);
	
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
