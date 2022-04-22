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
import conditions.Report_toAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeeTDO;
import tdo.Report_toTDO;
import conditions.EmployeeAttribute;
import dao.services.EmployeeService;
import tdo.EmployeeTDO;
import tdo.Report_toTDO;
import conditions.EmployeeAttribute;
import dao.services.EmployeeService;
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

public class Report_toServiceImpl extends dao.services.Report_toService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Report_toServiceImpl.class);
	
	
	
	public Dataset<Report_to> getReport_toList(
		Condition<EmployeeAttribute> lowerEmployee_condition,
		Condition<EmployeeAttribute> higherEmployee_condition){
			Report_toServiceImpl report_toService = this;
			EmployeeService employeeService = new EmployeeServiceImpl();  
			MutableBoolean lowerEmployee_refilter = new MutableBoolean(false);
			List<Dataset<Report_to>> datasetsPOJO = new ArrayList<Dataset<Report_to>>();
			boolean all_already_persisted = false;
			MutableBoolean higherEmployee_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Report_to> res_report_to_lowerEmployee;
			Dataset<Employee> res_Employee;
			
			
			//Join datasets or return 
			Dataset<Report_to> res = fullOuterJoinsReport_to(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employee> lonelyLowerEmployee = null;
			Dataset<Employee> lonelyHigherEmployee = null;
			
			List<Dataset<Employee>> lonelylowerEmployeeList = new ArrayList<Dataset<Employee>>();
			lonelylowerEmployeeList.add(employeeService.getEmployeeListInEmployeesFromMyMongoDB(lowerEmployee_condition, new MutableBoolean(false)));
			lonelyLowerEmployee = EmployeeService.fullOuterJoinsEmployee(lonelylowerEmployeeList);
			if(lonelyLowerEmployee != null) {
				res = fullLeftOuterJoinBetweenReport_toAndLowerEmployee(res, lonelyLowerEmployee);
			}	
		
			List<Dataset<Employee>> lonelyhigherEmployeeList = new ArrayList<Dataset<Employee>>();
			lonelyhigherEmployeeList.add(employeeService.getEmployeeListInEmployeesFromMyMongoDB(higherEmployee_condition, new MutableBoolean(false)));
			lonelyHigherEmployee = EmployeeService.fullOuterJoinsEmployee(lonelyhigherEmployeeList);
			if(lonelyHigherEmployee != null) {
				res = fullLeftOuterJoinBetweenReport_toAndHigherEmployee(res, lonelyHigherEmployee);
			}	
		
			
			if(lowerEmployee_refilter.booleanValue() || higherEmployee_refilter.booleanValue())
				res = res.filter((FilterFunction<Report_to>) r -> (lowerEmployee_condition == null || lowerEmployee_condition.evaluate(r.getLowerEmployee())) && (higherEmployee_condition == null || higherEmployee_condition.evaluate(r.getHigherEmployee())));
			
		
			return res;
		
		}
	
	public Dataset<Report_to> getReport_toListByLowerEmployeeCondition(
		Condition<EmployeeAttribute> lowerEmployee_condition
	){
		return getReport_toList(lowerEmployee_condition, null);
	}
	
	public Report_to getReport_toByLowerEmployee(Employee lowerEmployee) {
		Condition<EmployeeAttribute> cond = null;
		cond = Condition.simple(EmployeeAttribute.id, Operator.EQUALS, lowerEmployee.getId());
		Dataset<Report_to> res = getReport_toListByLowerEmployeeCondition(cond);
		List<Report_to> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Report_to> getReport_toListByHigherEmployeeCondition(
		Condition<EmployeeAttribute> higherEmployee_condition
	){
		return getReport_toList(null, higherEmployee_condition);
	}
	
	public Dataset<Report_to> getReport_toListByHigherEmployee(Employee higherEmployee) {
		Condition<EmployeeAttribute> cond = null;
		cond = Condition.simple(EmployeeAttribute.id, Operator.EQUALS, higherEmployee.getId());
		Dataset<Report_to> res = getReport_toListByHigherEmployeeCondition(cond);
	return res;
	}
	
	public void insertReport_to(Report_to report_to){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	
	
	
	
	
	public void deleteReport_toList(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition){
			//TODO
		}
	
	public void deleteReport_toListByLowerEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition
	){
		deleteReport_toList(lowerEmployee_condition, null);
	}
	
	public void deleteReport_toByLowerEmployee(pojo.Employee lowerEmployee) {
		// TODO using id for selecting
		return;
	}
	public void deleteReport_toListByHigherEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition
	){
		deleteReport_toList(null, higherEmployee_condition);
	}
	
	public void deleteReport_toListByHigherEmployee(pojo.Employee higherEmployee) {
		// TODO using id for selecting
		return;
	}
		
}
