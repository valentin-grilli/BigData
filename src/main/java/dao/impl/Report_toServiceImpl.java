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
	
	
	// Left side 'ReportsTo' of reference [reportToRef ]
	public Dataset<EmployeeTDO> getEmployeeTDOListLowerEmployeeInReportToRefInEmployeesFromMongoSchema(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = EmployeeServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeeTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeeTDO>) r -> {
				Set<EmployeeTDO> list_res = new HashSet<EmployeeTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeeTDO employee1 = new EmployeeTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employee.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employee1.setAddress(null);
						}else{
							employee1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employee1.setBirthDate(null);
						}else{
							employee1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employee1.setCity(null);
						}else{
							employee1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employee1.setCountry(null);
						}else{
							employee1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.id for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employee1.setId(null);
						}else{
							employee1.setId(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employee1.setExtension(null);
						}else{
							employee1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.firstname for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employee1.setFirstname(null);
						}else{
							employee1.setFirstname(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employee1.setHireDate(null);
						}else{
							employee1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employee1.setHomePhone(null);
						}else{
							employee1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.lastname for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employee1.setLastname(null);
						}else{
							employee1.setLastname(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employee1.setPhoto(null);
						}else{
							employee1.setPhoto(Util.getStringValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employee1.setPostalCode(null);
						}else{
							employee1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employee1.setRegion(null);
						}else{
							employee1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employee1.setSalary(null);
						}else{
							employee1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employee1.setTitle(null);
						}else{
							employee1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employee1.setNotes(null);
						}else{
							employee1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employee1.setPhotoPath(null);
						}else{
							employee1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employee1.setTitleOfCourtesy(null);
						}else{
							employee1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					
						// field  ReportsTo for reference reportToRef . Reference field : ReportsTo
					nestedRow =  r1;
					if(nestedRow != null) {
						employee1.setMongoSchema_Employees_reportToRef_ReportsTo(nestedRow.getAs("ReportsTo") == null ? null : nestedRow.getAs("ReportsTo").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employee1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeeTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	// Right side 'EmployeeID' of reference [reportToRef ]
	public Dataset<EmployeeTDO> getEmployeeTDOListHigherEmployeeInReportToRefInEmployeesFromMongoSchema(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeeServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeeTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeeTDO>) r -> {
				Set<EmployeeTDO> list_res = new HashSet<EmployeeTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeeTDO employee1 = new EmployeeTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employee.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employee1.setAddress(null);
						}else{
							employee1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employee1.setBirthDate(null);
						}else{
							employee1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employee1.setCity(null);
						}else{
							employee1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employee1.setCountry(null);
						}else{
							employee1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.id for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employee1.setId(null);
						}else{
							employee1.setId(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employee1.setExtension(null);
						}else{
							employee1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.firstname for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employee1.setFirstname(null);
						}else{
							employee1.setFirstname(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employee1.setHireDate(null);
						}else{
							employee1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employee1.setHomePhone(null);
						}else{
							employee1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.lastname for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employee1.setLastname(null);
						}else{
							employee1.setLastname(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employee1.setPhoto(null);
						}else{
							employee1.setPhoto(Util.getStringValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employee1.setPostalCode(null);
						}else{
							employee1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employee1.setRegion(null);
						}else{
							employee1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employee1.setSalary(null);
						}else{
							employee1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employee1.setTitle(null);
						}else{
							employee1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employee1.setNotes(null);
						}else{
							employee1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employee1.setPhotoPath(null);
						}else{
							employee1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employee1.setTitleOfCourtesy(null);
						}else{
							employee1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeID for reference reportToRef . Reference field : EmployeeID
					nestedRow =  r1;
					if(nestedRow != null) {
						employee1.setMongoSchema_Employees_reportToRef_EmployeeID(nestedRow.getAs("EmployeeID") == null ? null : nestedRow.getAs("EmployeeID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employee1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeeTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	
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
			// For role 'lowerEmployee' in reference 'reportToRef'. A->B Scenario
			higherEmployee_refilter = new MutableBoolean(false);
			Dataset<EmployeeTDO> employeeTDOreportToReflowerEmployee = report_toService.getEmployeeTDOListLowerEmployeeInReportToRefInEmployeesFromMongoSchema(lowerEmployee_condition, lowerEmployee_refilter);
			Dataset<EmployeeTDO> employeeTDOreportToRefhigherEmployee = report_toService.getEmployeeTDOListHigherEmployeeInReportToRefInEmployeesFromMongoSchema(higherEmployee_condition, higherEmployee_refilter);
			
			Dataset<Row> res_reportToRef_temp = employeeTDOreportToReflowerEmployee.join(employeeTDOreportToRefhigherEmployee
					.withColumnRenamed("id", "Employee_id")
					.withColumnRenamed("address", "Employee_address")
					.withColumnRenamed("birthDate", "Employee_birthDate")
					.withColumnRenamed("city", "Employee_city")
					.withColumnRenamed("country", "Employee_country")
					.withColumnRenamed("extension", "Employee_extension")
					.withColumnRenamed("firstname", "Employee_firstname")
					.withColumnRenamed("hireDate", "Employee_hireDate")
					.withColumnRenamed("homePhone", "Employee_homePhone")
					.withColumnRenamed("lastname", "Employee_lastname")
					.withColumnRenamed("photo", "Employee_photo")
					.withColumnRenamed("postalCode", "Employee_postalCode")
					.withColumnRenamed("region", "Employee_region")
					.withColumnRenamed("salary", "Employee_salary")
					.withColumnRenamed("title", "Employee_title")
					.withColumnRenamed("notes", "Employee_notes")
					.withColumnRenamed("photoPath", "Employee_photoPath")
					.withColumnRenamed("titleOfCourtesy", "Employee_titleOfCourtesy")
					.withColumnRenamed("logEvents", "Employee_logEvents"),
					employeeTDOreportToReflowerEmployee.col("mongoSchema_Employees_reportToRef_ReportsTo").equalTo(employeeTDOreportToRefhigherEmployee.col("mongoSchema_Employees_reportToRef_EmployeeID")));
		
			Dataset<Report_to> res_reportToRef = res_reportToRef_temp.map(
				(MapFunction<Row, Report_to>) r -> {
					Report_to res = new Report_to();
					Employee A = new Employee();
					Employee B = new Employee();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setAddress(Util.getStringValue(r.getAs("address")));
					A.setBirthDate(Util.getLocalDateValue(r.getAs("birthDate")));
					A.setCity(Util.getStringValue(r.getAs("city")));
					A.setCountry(Util.getStringValue(r.getAs("country")));
					A.setExtension(Util.getStringValue(r.getAs("extension")));
					A.setFirstname(Util.getStringValue(r.getAs("firstname")));
					A.setHireDate(Util.getLocalDateValue(r.getAs("hireDate")));
					A.setHomePhone(Util.getStringValue(r.getAs("homePhone")));
					A.setLastname(Util.getStringValue(r.getAs("lastname")));
					A.setPhoto(Util.getStringValue(r.getAs("photo")));
					A.setPostalCode(Util.getStringValue(r.getAs("postalCode")));
					A.setRegion(Util.getStringValue(r.getAs("region")));
					A.setSalary(Util.getDoubleValue(r.getAs("salary")));
					A.setTitle(Util.getStringValue(r.getAs("title")));
					A.setNotes(Util.getStringValue(r.getAs("notes")));
					A.setPhotoPath(Util.getStringValue(r.getAs("photoPath")));
					A.setTitleOfCourtesy(Util.getStringValue(r.getAs("titleOfCourtesy")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("Employee_id")));
					B.setAddress(Util.getStringValue(r.getAs("Employee_address")));
					B.setBirthDate(Util.getLocalDateValue(r.getAs("Employee_birthDate")));
					B.setCity(Util.getStringValue(r.getAs("Employee_city")));
					B.setCountry(Util.getStringValue(r.getAs("Employee_country")));
					B.setExtension(Util.getStringValue(r.getAs("Employee_extension")));
					B.setFirstname(Util.getStringValue(r.getAs("Employee_firstname")));
					B.setHireDate(Util.getLocalDateValue(r.getAs("Employee_hireDate")));
					B.setHomePhone(Util.getStringValue(r.getAs("Employee_homePhone")));
					B.setLastname(Util.getStringValue(r.getAs("Employee_lastname")));
					B.setPhoto(Util.getStringValue(r.getAs("Employee_photo")));
					B.setPostalCode(Util.getStringValue(r.getAs("Employee_postalCode")));
					B.setRegion(Util.getStringValue(r.getAs("Employee_region")));
					B.setSalary(Util.getDoubleValue(r.getAs("Employee_salary")));
					B.setTitle(Util.getStringValue(r.getAs("Employee_title")));
					B.setNotes(Util.getStringValue(r.getAs("Employee_notes")));
					B.setPhotoPath(Util.getStringValue(r.getAs("Employee_photoPath")));
					B.setTitleOfCourtesy(Util.getStringValue(r.getAs("Employee_titleOfCourtesy")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Employee_logEvents")));
						
					res.setLowerEmployee(A);
					res.setHigherEmployee(B);
					return res;
				},Encoders.bean(Report_to.class)
			);
		
			datasetsPOJO.add(res_reportToRef);
		
			
			Dataset<Report_to> res_report_to_lowerEmployee;
			Dataset<Employee> res_Employee;
			
			
			//Join datasets or return 
			Dataset<Report_to> res = fullOuterJoinsReport_to(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employee> lonelyLowerEmployee = null;
			Dataset<Employee> lonelyHigherEmployee = null;
			
		
		
			
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
		insertReport_toInRefStructEmployeesInMyMongoDB(report_to);
	}
	
	
	
	public 	boolean insertReport_toInRefStructEmployeesInMyMongoDB(Report_to report_to){
	 	// Rel 'report_to' Insert in reference structure 'Employees'
		Employee employeeLowerEmployee = report_to.getLowerEmployee();
		Employee employeeHigherEmployee = report_to.getHigherEmployee();
	
		Bson filter = new Document();
		Bson updateOp;
		updateOp = set("ReportsTo",employeeHigherEmployee==null?null:employeeHigherEmployee.getId());
		filter = eq("EmployeeID",employeeLowerEmployee.getId());
		DBConnectionMgr.update(filter, updateOp, "Employees", "myMongoDB");
		return true;
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
