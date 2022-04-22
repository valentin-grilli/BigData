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
import conditions.Are_inAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeeTDO;
import tdo.Are_inTDO;
import conditions.EmployeeAttribute;
import dao.services.EmployeeService;
import tdo.TerritoryTDO;
import tdo.Are_inTDO;
import conditions.TerritoryAttribute;
import dao.services.TerritoryService;
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

public class Are_inServiceImpl extends dao.services.Are_inService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Are_inServiceImpl.class);
	
	
	
	public Dataset<Are_in> getAre_inList(
		Condition<EmployeeAttribute> employee_condition,
		Condition<TerritoryAttribute> territory_condition){
			Are_inServiceImpl are_inService = this;
			EmployeeService employeeService = new EmployeeServiceImpl();  
			TerritoryService territoryService = new TerritoryServiceImpl();
			MutableBoolean employee_refilter = new MutableBoolean(false);
			List<Dataset<Are_in>> datasetsPOJO = new ArrayList<Dataset<Are_in>>();
			boolean all_already_persisted = false;
			MutableBoolean territory_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Are_in> res_are_in_employee;
			Dataset<Employee> res_Employee;
			
			
			//Join datasets or return 
			Dataset<Are_in> res = fullOuterJoinsAre_in(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employee> lonelyEmployee = null;
			Dataset<Territory> lonelyTerritory = null;
			
			List<Dataset<Employee>> lonelyemployeeList = new ArrayList<Dataset<Employee>>();
			lonelyemployeeList.add(employeeService.getEmployeeListInEmployeesFromMyMongoDB(employee_condition, new MutableBoolean(false)));
			lonelyEmployee = EmployeeService.fullOuterJoinsEmployee(lonelyemployeeList);
			if(lonelyEmployee != null) {
				res = fullLeftOuterJoinBetweenAre_inAndEmployee(res, lonelyEmployee);
			}	
		
			List<Dataset<Territory>> lonelyterritoryList = new ArrayList<Dataset<Territory>>();
			lonelyterritoryList.add(territoryService.getTerritoryListInEmployeesFromMyMongoDB(territory_condition, new MutableBoolean(false)));
			lonelyTerritory = TerritoryService.fullOuterJoinsTerritory(lonelyterritoryList);
			if(lonelyTerritory != null) {
				res = fullLeftOuterJoinBetweenAre_inAndTerritory(res, lonelyTerritory);
			}	
		
			
			if(employee_refilter.booleanValue() || territory_refilter.booleanValue())
				res = res.filter((FilterFunction<Are_in>) r -> (employee_condition == null || employee_condition.evaluate(r.getEmployee())) && (territory_condition == null || territory_condition.evaluate(r.getTerritory())));
			
		
			return res;
		
		}
	
	public Dataset<Are_in> getAre_inListByEmployeeCondition(
		Condition<EmployeeAttribute> employee_condition
	){
		return getAre_inList(employee_condition, null);
	}
	
	public Dataset<Are_in> getAre_inListByEmployee(Employee employee) {
		Condition<EmployeeAttribute> cond = null;
		cond = Condition.simple(EmployeeAttribute.id, Operator.EQUALS, employee.getId());
		Dataset<Are_in> res = getAre_inListByEmployeeCondition(cond);
	return res;
	}
	public Dataset<Are_in> getAre_inListByTerritoryCondition(
		Condition<TerritoryAttribute> territory_condition
	){
		return getAre_inList(null, territory_condition);
	}
	
	public Dataset<Are_in> getAre_inListByTerritory(Territory territory) {
		Condition<TerritoryAttribute> cond = null;
		cond = Condition.simple(TerritoryAttribute.id, Operator.EQUALS, territory.getId());
		Dataset<Are_in> res = getAre_inListByTerritoryCondition(cond);
	return res;
	}
	
	public void insertAre_in(Are_in are_in){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	
	
	
	
	
	public void deleteAre_inList(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.TerritoryAttribute> territory_condition){
			//TODO
		}
	
	public void deleteAre_inListByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteAre_inList(employee_condition, null);
	}
	
	public void deleteAre_inListByEmployee(pojo.Employee employee) {
		// TODO using id for selecting
		return;
	}
	public void deleteAre_inListByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition
	){
		deleteAre_inList(null, territory_condition);
	}
	
	public void deleteAre_inListByTerritory(pojo.Territory territory) {
		// TODO using id for selecting
		return;
	}
		
}
