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
import conditions.ReportsToAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeesTDO;
import tdo.ReportsToTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
import tdo.EmployeesTDO;
import tdo.ReportsToTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
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

public class ReportsToServiceImpl extends dao.services.ReportsToService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReportsToServiceImpl.class);
	
	
	// Left side 'ReportsTo' of reference [manager ]
	public Dataset<EmployeesTDO> getEmployeesTDOListSubordoneeInManagerInEmployeesFromMongoDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = EmployeesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeesTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeesTDO>) r -> {
				Set<EmployeesTDO> list_res = new HashSet<EmployeesTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeesTDO employees1 = new EmployeesTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employees.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employees1.setEmployeeID(null);
						}else{
							employees1.setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employees1.setLastName(null);
						}else{
							employees1.setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employees1.setFirstName(null);
						}else{
							employees1.setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employees1.setTitle(null);
						}else{
							employees1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employees1.setTitleOfCourtesy(null);
						}else{
							employees1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employees1.setBirthDate(null);
						}else{
							employees1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employees1.setHireDate(null);
						}else{
							employees1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employees1.setHomePhone(null);
						}else{
							employees1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employees1.setExtension(null);
						}else{
							employees1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employees1.setPhoto(null);
						}else{
							employees1.setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employees1.setNotes(null);
						}else{
							employees1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employees1.setPhotoPath(null);
						}else{
							employees1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employees1.setSalary(null);
						}else{
							employees1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employees1.setAddress(null);
						}else{
							employees1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employees1.setCity(null);
						}else{
							employees1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employees1.setRegion(null);
						}else{
							employees1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employees1.setPostalCode(null);
						}else{
							employees1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employees1.setCountry(null);
						}else{
							employees1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					
						// field  ReportsTo for reference manager . Reference field : ReportsTo
					nestedRow =  r1;
					if(nestedRow != null) {
						employees1.setMongoDB_Employees_manager_ReportsTo(nestedRow.getAs("ReportsTo") == null ? null : nestedRow.getAs("ReportsTo").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employees1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeesTDO.class));
		res= res.dropDuplicates(new String[]{"employeeID"});
		return res;
	}
	
	// Right side 'EmployeeID' of reference [manager ]
	public Dataset<EmployeesTDO> getEmployeesTDOListBossInManagerInEmployeesFromMongoDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeesTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeesTDO>) r -> {
				Set<EmployeesTDO> list_res = new HashSet<EmployeesTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeesTDO employees1 = new EmployeesTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employees.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employees1.setEmployeeID(null);
						}else{
							employees1.setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employees1.setLastName(null);
						}else{
							employees1.setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employees1.setFirstName(null);
						}else{
							employees1.setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employees1.setTitle(null);
						}else{
							employees1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employees1.setTitleOfCourtesy(null);
						}else{
							employees1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employees1.setBirthDate(null);
						}else{
							employees1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employees1.setHireDate(null);
						}else{
							employees1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employees1.setHomePhone(null);
						}else{
							employees1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employees1.setExtension(null);
						}else{
							employees1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employees1.setPhoto(null);
						}else{
							employees1.setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employees1.setNotes(null);
						}else{
							employees1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employees1.setPhotoPath(null);
						}else{
							employees1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employees1.setSalary(null);
						}else{
							employees1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employees1.setAddress(null);
						}else{
							employees1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employees1.setCity(null);
						}else{
							employees1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employees1.setRegion(null);
						}else{
							employees1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employees1.setPostalCode(null);
						}else{
							employees1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employees1.setCountry(null);
						}else{
							employees1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeID for reference manager . Reference field : EmployeeID
					nestedRow =  r1;
					if(nestedRow != null) {
						employees1.setMongoDB_Employees_manager_EmployeeID(nestedRow.getAs("EmployeeID") == null ? null : nestedRow.getAs("EmployeeID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employees1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeesTDO.class));
		res= res.dropDuplicates(new String[]{"employeeID"});
		return res;
	}
	
	
	
	
	public Dataset<ReportsTo> getReportsToList(
		Condition<EmployeesAttribute> subordonee_condition,
		Condition<EmployeesAttribute> boss_condition){
			ReportsToServiceImpl reportsToService = this;
			EmployeesService employeesService = new EmployeesServiceImpl();  
			MutableBoolean subordonee_refilter = new MutableBoolean(false);
			List<Dataset<ReportsTo>> datasetsPOJO = new ArrayList<Dataset<ReportsTo>>();
			boolean all_already_persisted = false;
			MutableBoolean boss_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'subordonee' in reference 'manager'. A->B Scenario
			boss_refilter = new MutableBoolean(false);
			Dataset<EmployeesTDO> employeesTDOmanagersubordonee = reportsToService.getEmployeesTDOListSubordoneeInManagerInEmployeesFromMongoDB(subordonee_condition, subordonee_refilter);
			Dataset<EmployeesTDO> employeesTDOmanagerboss = reportsToService.getEmployeesTDOListBossInManagerInEmployeesFromMongoDB(boss_condition, boss_refilter);
			
			Dataset<Row> res_manager_temp = employeesTDOmanagersubordonee.join(employeesTDOmanagerboss
					.withColumnRenamed("employeeID", "Employees_employeeID")
					.withColumnRenamed("lastName", "Employees_lastName")
					.withColumnRenamed("firstName", "Employees_firstName")
					.withColumnRenamed("title", "Employees_title")
					.withColumnRenamed("titleOfCourtesy", "Employees_titleOfCourtesy")
					.withColumnRenamed("birthDate", "Employees_birthDate")
					.withColumnRenamed("hireDate", "Employees_hireDate")
					.withColumnRenamed("address", "Employees_address")
					.withColumnRenamed("city", "Employees_city")
					.withColumnRenamed("region", "Employees_region")
					.withColumnRenamed("postalCode", "Employees_postalCode")
					.withColumnRenamed("country", "Employees_country")
					.withColumnRenamed("homePhone", "Employees_homePhone")
					.withColumnRenamed("extension", "Employees_extension")
					.withColumnRenamed("photo", "Employees_photo")
					.withColumnRenamed("notes", "Employees_notes")
					.withColumnRenamed("photoPath", "Employees_photoPath")
					.withColumnRenamed("salary", "Employees_salary")
					.withColumnRenamed("logEvents", "Employees_logEvents"),
					employeesTDOmanagersubordonee.col("mongoDB_Employees_manager_ReportsTo").equalTo(employeesTDOmanagerboss.col("mongoDB_Employees_manager_EmployeeID")));
		
			Dataset<ReportsTo> res_manager = res_manager_temp.map(
				(MapFunction<Row, ReportsTo>) r -> {
					ReportsTo res = new ReportsTo();
					Employees A = new Employees();
					Employees B = new Employees();
					A.setEmployeeID(Util.getIntegerValue(r.getAs("employeeID")));
					A.setLastName(Util.getStringValue(r.getAs("lastName")));
					A.setFirstName(Util.getStringValue(r.getAs("firstName")));
					A.setTitle(Util.getStringValue(r.getAs("title")));
					A.setTitleOfCourtesy(Util.getStringValue(r.getAs("titleOfCourtesy")));
					A.setBirthDate(Util.getLocalDateValue(r.getAs("birthDate")));
					A.setHireDate(Util.getLocalDateValue(r.getAs("hireDate")));
					A.setAddress(Util.getStringValue(r.getAs("address")));
					A.setCity(Util.getStringValue(r.getAs("city")));
					A.setRegion(Util.getStringValue(r.getAs("region")));
					A.setPostalCode(Util.getStringValue(r.getAs("postalCode")));
					A.setCountry(Util.getStringValue(r.getAs("country")));
					A.setHomePhone(Util.getStringValue(r.getAs("homePhone")));
					A.setExtension(Util.getStringValue(r.getAs("extension")));
					A.setPhoto(Util.getByteArrayValue(r.getAs("photo")));
					A.setNotes(Util.getStringValue(r.getAs("notes")));
					A.setPhotoPath(Util.getStringValue(r.getAs("photoPath")));
					A.setSalary(Util.getDoubleValue(r.getAs("salary")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setEmployeeID(Util.getIntegerValue(r.getAs("Employees_employeeID")));
					B.setLastName(Util.getStringValue(r.getAs("Employees_lastName")));
					B.setFirstName(Util.getStringValue(r.getAs("Employees_firstName")));
					B.setTitle(Util.getStringValue(r.getAs("Employees_title")));
					B.setTitleOfCourtesy(Util.getStringValue(r.getAs("Employees_titleOfCourtesy")));
					B.setBirthDate(Util.getLocalDateValue(r.getAs("Employees_birthDate")));
					B.setHireDate(Util.getLocalDateValue(r.getAs("Employees_hireDate")));
					B.setAddress(Util.getStringValue(r.getAs("Employees_address")));
					B.setCity(Util.getStringValue(r.getAs("Employees_city")));
					B.setRegion(Util.getStringValue(r.getAs("Employees_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Employees_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Employees_country")));
					B.setHomePhone(Util.getStringValue(r.getAs("Employees_homePhone")));
					B.setExtension(Util.getStringValue(r.getAs("Employees_extension")));
					B.setPhoto(Util.getByteArrayValue(r.getAs("Employees_photo")));
					B.setNotes(Util.getStringValue(r.getAs("Employees_notes")));
					B.setPhotoPath(Util.getStringValue(r.getAs("Employees_photoPath")));
					B.setSalary(Util.getDoubleValue(r.getAs("Employees_salary")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Employees_logEvents")));
						
					res.setSubordonee(A);
					res.setBoss(B);
					return res;
				},Encoders.bean(ReportsTo.class)
			);
		
			datasetsPOJO.add(res_manager);
		
			
			Dataset<ReportsTo> res_reportsTo_subordonee;
			Dataset<Employees> res_Employees;
			
			
			//Join datasets or return 
			Dataset<ReportsTo> res = fullOuterJoinsReportsTo(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employees> lonelySubordonee = null;
			Dataset<Employees> lonelyBoss = null;
			
		
		
			
			if(subordonee_refilter.booleanValue() || boss_refilter.booleanValue())
				res = res.filter((FilterFunction<ReportsTo>) r -> (subordonee_condition == null || subordonee_condition.evaluate(r.getSubordonee())) && (boss_condition == null || boss_condition.evaluate(r.getBoss())));
			
		
			return res;
		
		}
	
	public Dataset<ReportsTo> getReportsToListBySubordoneeCondition(
		Condition<EmployeesAttribute> subordonee_condition
	){
		return getReportsToList(subordonee_condition, null);
	}
	
	public ReportsTo getReportsToBySubordonee(Employees subordonee) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, subordonee.getEmployeeID());
		Dataset<ReportsTo> res = getReportsToListBySubordoneeCondition(cond);
		List<ReportsTo> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<ReportsTo> getReportsToListByBossCondition(
		Condition<EmployeesAttribute> boss_condition
	){
		return getReportsToList(null, boss_condition);
	}
	
	public Dataset<ReportsTo> getReportsToListByBoss(Employees boss) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, boss.getEmployeeID());
		Dataset<ReportsTo> res = getReportsToListByBossCondition(cond);
	return res;
	}
	
	public void insertReportsTo(ReportsTo reportsTo){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertReportsToInRefStructEmployeesInMyMongoDB(reportsTo);
	}
	
	
	
	public 	boolean insertReportsToInRefStructEmployeesInMyMongoDB(ReportsTo reportsTo){
	 	// Rel 'reportsTo' Insert in reference structure 'Employees'
		Employees employeesSubordonee = reportsTo.getSubordonee();
		Employees employeesBoss = reportsTo.getBoss();
	
		Bson filter = new Document();
		Bson updateOp;
		updateOp = set("ReportsTo",employeesBoss==null?null:employeesBoss.getEmployeeID());
		filter = eq("EmployeeID",employeesSubordonee.getEmployeeID());
		DBConnectionMgr.update(filter, updateOp, "Employees", "myMongoDB");
		return true;
	}
	
	
	
	
	public void deleteReportsToList(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition){
			//TODO
		}
	
	public void deleteReportsToListBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteReportsToList(subordonee_condition, null);
	}
	
	public void deleteReportsToBySubordonee(pojo.Employees subordonee) {
		// TODO using id for selecting
		return;
	}
	public void deleteReportsToListByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteReportsToList(null, boss_condition);
	}
	
	public void deleteReportsToListByBoss(pojo.Employees boss) {
		// TODO using id for selecting
		return;
	}
		
}
