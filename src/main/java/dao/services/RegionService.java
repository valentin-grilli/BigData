package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Region;
import conditions.RegionAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.RegionAttribute;
import pojo.Contains;
import conditions.TerritoryAttribute;
import pojo.Territory;

public abstract class RegionService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegionService.class);
	protected ContainsService containsService = new dao.impl.ContainsServiceImpl();
	


	public static enum ROLE_NAME {
		CONTAINS_REGION
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.CONTAINS_REGION, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public RegionService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public RegionService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Region> getRegionList(){
		return getRegionList(null);
	}
	
	public Dataset<Region> getRegionList(conditions.Condition<conditions.RegionAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Region>> datasets = new ArrayList<Dataset<Region>>();
		Dataset<Region> d = null;
		d = getRegionListInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsRegion(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Region>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Region> getRegionListInEmployeesFromMyMongoDB(conditions.Condition<conditions.RegionAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Region getRegionById(Integer id){
		Condition cond;
		cond = Condition.simple(RegionAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Region> res = getRegionList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Region> getRegionListById(Integer id) {
		return getRegionList(conditions.Condition.simple(conditions.RegionAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Region> getRegionListByDescription(String description) {
		return getRegionList(conditions.Condition.simple(conditions.RegionAttribute.description, conditions.Operator.EQUALS, description));
	}
	
	
	
	public static Dataset<Region> fullOuterJoinsRegion(List<Dataset<Region>> datasetsPOJO) {
		return fullOuterJoinsRegion(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Region> fullLeftOuterJoinsRegion(List<Dataset<Region>> datasetsPOJO) {
		return fullOuterJoinsRegion(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Region> fullOuterJoinsRegion(List<Dataset<Region>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Region> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Region] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("description", "description_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("description", "description_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Region] objects"); 
			d = res.map((MapFunction<Row, Region>) r -> {
					Region region_res = new Region();
					
					// attribute 'Region.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					region_res.setId(firstNotNull_id);
					
					// attribute 'Region.description'
					String firstNotNull_description = Util.getStringValue(r.getAs("description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String description2 = Util.getStringValue(r.getAs("description_" + i));
						if (firstNotNull_description != null && description2 != null && !firstNotNull_description.equals(description2)) {
							region_res.addLogEvent("Data consistency problem for [Region - id :"+region_res.getId()+"]: different values found for attribute 'Region.description': " + firstNotNull_description + " and " + description2 + "." );
							logger.warn("Data consistency problem for [Region - id :"+region_res.getId()+"]: different values found for attribute 'Region.description': " + firstNotNull_description + " and " + description2 + "." );
						}
						if (firstNotNull_description == null && description2 != null) {
							firstNotNull_description = description2;
						}
					}
					region_res.setDescription(firstNotNull_description);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							region_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							region_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return region_res;
				}, Encoders.bean(Region.class));
			return d;
	}
	
	
	
	public Region getRegion(Region.contains role, Territory territory) {
		if(role != null) {
			if(role.equals(Region.contains.region))
				return getRegionInContainsByTerritory(territory);
		}
		return null;
	}
	
	public Dataset<Region> getRegionList(Region.contains role, Condition<TerritoryAttribute> condition) {
		if(role != null) {
			if(role.equals(Region.contains.region))
				return getRegionListInContainsByTerritoryCondition(condition);
		}
		return null;
	}
	
	public Dataset<Region> getRegionList(Region.contains role, Condition<TerritoryAttribute> condition1, Condition<RegionAttribute> condition2) {
		if(role != null) {
			if(role.equals(Region.contains.region))
				return getRegionListInContains(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	
	
	public abstract Dataset<Region> getRegionListInContains(conditions.Condition<conditions.TerritoryAttribute> territory_condition,conditions.Condition<conditions.RegionAttribute> region_condition);
	
	public Dataset<Region> getRegionListInContainsByTerritoryCondition(conditions.Condition<conditions.TerritoryAttribute> territory_condition){
		return getRegionListInContains(territory_condition, null);
	}
	
	public Region getRegionInContainsByTerritory(pojo.Territory territory){
		if(territory == null)
			return null;
	
		Condition c;
		c=Condition.simple(TerritoryAttribute.id,Operator.EQUALS, territory.getId());
		Dataset<Region> res = getRegionListInContainsByTerritoryCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Region> getRegionListInContainsByRegionCondition(conditions.Condition<conditions.RegionAttribute> region_condition){
		return getRegionListInContains(null, region_condition);
	}
	
	public abstract boolean insertRegion(
		Region region,
		 List<Territory> territoryContains);
	
	public abstract boolean insertRegionInEmployeesFromMyMongoDB(Region region); 
	private boolean inUpdateMethod = false;
	private List<Row> allRegionIdList = null;
	public abstract void updateRegionList(conditions.Condition<conditions.RegionAttribute> condition, conditions.SetClause<conditions.RegionAttribute> set);
	
	public void updateRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public abstract void updateRegionListInContains(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.RegionAttribute> set
	);
	
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
	
	
	public abstract void deleteRegionList(conditions.Condition<conditions.RegionAttribute> condition);
	
	public void deleteRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public abstract void deleteRegionListInContains(	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition);
	
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
