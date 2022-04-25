package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Territory;
import conditions.TerritoryAttribute;
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
import conditions.TerritoryAttribute;
import pojo.Contains;
import conditions.RegionAttribute;
import pojo.Region;
import conditions.TerritoryAttribute;
import pojo.Are_in;
import conditions.EmployeeAttribute;
import pojo.Employee;

public abstract class TerritoryService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TerritoryService.class);
	protected ContainsService containsService = new dao.impl.ContainsServiceImpl();
	protected Are_inService are_inService = new dao.impl.Are_inServiceImpl();
	


	public static enum ROLE_NAME {
		CONTAINS_TERRITORY, ARE_IN_TERRITORY
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.CONTAINS_TERRITORY, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.ARE_IN_TERRITORY, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public TerritoryService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public TerritoryService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Territory> getTerritoryList(){
		return getTerritoryList(null);
	}
	
	public Dataset<Territory> getTerritoryList(conditions.Condition<conditions.TerritoryAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Territory>> datasets = new ArrayList<Dataset<Territory>>();
		Dataset<Territory> d = null;
		d = getTerritoryListInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsTerritory(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Territory>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Territory> getTerritoryListInEmployeesFromMyMongoDB(conditions.Condition<conditions.TerritoryAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Territory getTerritoryById(Integer id){
		Condition cond;
		cond = Condition.simple(TerritoryAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Territory> res = getTerritoryList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Territory> getTerritoryListById(Integer id) {
		return getTerritoryList(conditions.Condition.simple(conditions.TerritoryAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Territory> getTerritoryListByDescription(String description) {
		return getTerritoryList(conditions.Condition.simple(conditions.TerritoryAttribute.description, conditions.Operator.EQUALS, description));
	}
	
	
	
	public static Dataset<Territory> fullOuterJoinsTerritory(List<Dataset<Territory>> datasetsPOJO) {
		return fullOuterJoinsTerritory(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Territory> fullLeftOuterJoinsTerritory(List<Dataset<Territory>> datasetsPOJO) {
		return fullOuterJoinsTerritory(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Territory> fullOuterJoinsTerritory(List<Dataset<Territory>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Territory> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Territory] objects",joinMode,datasetsPOJO.size());
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
			logger.debug("Start transforming Row objects to [Territory] objects"); 
			d = res.map((MapFunction<Row, Territory>) r -> {
					Territory territory_res = new Territory();
					
					// attribute 'Territory.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					territory_res.setId(firstNotNull_id);
					
					// attribute 'Territory.description'
					String firstNotNull_description = Util.getStringValue(r.getAs("description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String description2 = Util.getStringValue(r.getAs("description_" + i));
						if (firstNotNull_description != null && description2 != null && !firstNotNull_description.equals(description2)) {
							territory_res.addLogEvent("Data consistency problem for [Territory - id :"+territory_res.getId()+"]: different values found for attribute 'Territory.description': " + firstNotNull_description + " and " + description2 + "." );
							logger.warn("Data consistency problem for [Territory - id :"+territory_res.getId()+"]: different values found for attribute 'Territory.description': " + firstNotNull_description + " and " + description2 + "." );
						}
						if (firstNotNull_description == null && description2 != null) {
							firstNotNull_description = description2;
						}
					}
					territory_res.setDescription(firstNotNull_description);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							territory_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							territory_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return territory_res;
				}, Encoders.bean(Territory.class));
			return d;
	}
	
	
	
	public Dataset<Territory> getTerritoryList(Territory.contains role, Region region) {
		if(role != null) {
			if(role.equals(Territory.contains.territory))
				return getTerritoryListInContainsByRegion(region);
		}
		return null;
	}
	
	public Dataset<Territory> getTerritoryList(Territory.contains role, Condition<RegionAttribute> condition) {
		if(role != null) {
			if(role.equals(Territory.contains.territory))
				return getTerritoryListInContainsByRegionCondition(condition);
		}
		return null;
	}
	
	public Dataset<Territory> getTerritoryList(Territory.contains role, Condition<TerritoryAttribute> condition1, Condition<RegionAttribute> condition2) {
		if(role != null) {
			if(role.equals(Territory.contains.territory))
				return getTerritoryListInContains(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Territory> getTerritoryList(Territory.are_in role, Employee employee) {
		if(role != null) {
			if(role.equals(Territory.are_in.territory))
				return getTerritoryListInAre_inByEmployee(employee);
		}
		return null;
	}
	
	public Dataset<Territory> getTerritoryList(Territory.are_in role, Condition<EmployeeAttribute> condition) {
		if(role != null) {
			if(role.equals(Territory.are_in.territory))
				return getTerritoryListInAre_inByEmployeeCondition(condition);
		}
		return null;
	}
	
	public Dataset<Territory> getTerritoryList(Territory.are_in role, Condition<EmployeeAttribute> condition1, Condition<TerritoryAttribute> condition2) {
		if(role != null) {
			if(role.equals(Territory.are_in.territory))
				return getTerritoryListInAre_in(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	
	
	public abstract Dataset<Territory> getTerritoryListInContains(conditions.Condition<conditions.TerritoryAttribute> territory_condition,conditions.Condition<conditions.RegionAttribute> region_condition);
	
	public Dataset<Territory> getTerritoryListInContainsByTerritoryCondition(conditions.Condition<conditions.TerritoryAttribute> territory_condition){
		return getTerritoryListInContains(territory_condition, null);
	}
	public Dataset<Territory> getTerritoryListInContainsByRegionCondition(conditions.Condition<conditions.RegionAttribute> region_condition){
		return getTerritoryListInContains(null, region_condition);
	}
	
	public Dataset<Territory> getTerritoryListInContainsByRegion(pojo.Region region){
		if(region == null)
			return null;
	
		Condition c;
		c=Condition.simple(RegionAttribute.id,Operator.EQUALS, region.getId());
		Dataset<Territory> res = getTerritoryListInContainsByRegionCondition(c);
		return res;
	}
	
	public abstract Dataset<Territory> getTerritoryListInAre_in(conditions.Condition<conditions.EmployeeAttribute> employee_condition,conditions.Condition<conditions.TerritoryAttribute> territory_condition);
	
	public Dataset<Territory> getTerritoryListInAre_inByEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> employee_condition){
		return getTerritoryListInAre_in(employee_condition, null);
	}
	
	public Dataset<Territory> getTerritoryListInAre_inByEmployee(pojo.Employee employee){
		if(employee == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeeAttribute.id,Operator.EQUALS, employee.getId());
		Dataset<Territory> res = getTerritoryListInAre_inByEmployeeCondition(c);
		return res;
	}
	
	public Dataset<Territory> getTerritoryListInAre_inByTerritoryCondition(conditions.Condition<conditions.TerritoryAttribute> territory_condition){
		return getTerritoryListInAre_in(null, territory_condition);
	}
	
	public abstract boolean insertTerritory(
		Territory territory,
		Region	regionContains);
	
	private boolean inUpdateMethod = false;
	private List<Row> allTerritoryIdList = null;
	public abstract void updateTerritoryList(conditions.Condition<conditions.TerritoryAttribute> condition, conditions.SetClause<conditions.TerritoryAttribute> set);
	
	public void updateTerritory(pojo.Territory territory) {
		//TODO using the id
		return;
	}
	public abstract void updateTerritoryListInContains(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.TerritoryAttribute> set
	);
	
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
	
	public abstract void updateTerritoryListInAre_in(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		
		conditions.SetClause<conditions.TerritoryAttribute> set
	);
	
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
	
	
	public abstract void deleteTerritoryList(conditions.Condition<conditions.TerritoryAttribute> condition);
	
	public void deleteTerritory(pojo.Territory territory) {
		//TODO using the id
		return;
	}
	public abstract void deleteTerritoryListInContains(	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition);
	
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
	
	public abstract void deleteTerritoryListInAre_in(	
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition);
	
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
