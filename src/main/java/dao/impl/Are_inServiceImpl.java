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
	
	// method accessing the embedded object territories mapped to role employee
	public Dataset<Are_in> getAre_inListInmongoSchemaEmployeesterritories(Condition<EmployeeAttribute> employee_condition, Condition<TerritoryAttribute> territory_condition, MutableBoolean employee_refilter, MutableBoolean territory_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = EmployeeServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(employee_condition ,employee_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = TerritoryServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(territory_condition ,territory_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
		
			Dataset<Are_in> res = dataset.flatMap((FlatMapFunction<Row, Are_in>) r -> {
					List<Are_in> list_res = new ArrayList<Are_in>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Are_in are_in1 = new Are_in();
					are_in1.setEmployee(new Employee());
					are_in1.setTerritory(new Territory());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employee.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address")==null)
							are_in1.getEmployee().setAddress(null);
						else{
							are_in1.getEmployee().setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate")==null)
							are_in1.getEmployee().setBirthDate(null);
						else{
							are_in1.getEmployee().setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City")==null)
							are_in1.getEmployee().setCity(null);
						else{
							are_in1.getEmployee().setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country")==null)
							are_in1.getEmployee().setCountry(null);
						else{
							are_in1.getEmployee().setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.id for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID")==null)
							are_in1.getEmployee().setId(null);
						else{
							are_in1.getEmployee().setId(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension")==null)
							are_in1.getEmployee().setExtension(null);
						else{
							are_in1.getEmployee().setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.firstname for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName")==null)
							are_in1.getEmployee().setFirstname(null);
						else{
							are_in1.getEmployee().setFirstname(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate")==null)
							are_in1.getEmployee().setHireDate(null);
						else{
							are_in1.getEmployee().setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone")==null)
							are_in1.getEmployee().setHomePhone(null);
						else{
							are_in1.getEmployee().setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.lastname for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName")==null)
							are_in1.getEmployee().setLastname(null);
						else{
							are_in1.getEmployee().setLastname(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo")==null)
							are_in1.getEmployee().setPhoto(null);
						else{
							are_in1.getEmployee().setPhoto(Util.getStringValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode")==null)
							are_in1.getEmployee().setPostalCode(null);
						else{
							are_in1.getEmployee().setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region")==null)
							are_in1.getEmployee().setRegion(null);
						else{
							are_in1.getEmployee().setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary")==null)
							are_in1.getEmployee().setSalary(null);
						else{
							are_in1.getEmployee().setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title")==null)
							are_in1.getEmployee().setTitle(null);
						else{
							are_in1.getEmployee().setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes")==null)
							are_in1.getEmployee().setNotes(null);
						else{
							are_in1.getEmployee().setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath")==null)
							are_in1.getEmployee().setPhotoPath(null);
						else{
							are_in1.getEmployee().setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy")==null)
							are_in1.getEmployee().setTitleOfCourtesy(null);
						else{
							are_in1.getEmployee().setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Are_in are_in2 = (Are_in) are_in1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territory.description for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									are_in2.getTerritory().setDescription(null);
								else{
									are_in2.getTerritory().setDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territory.id for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									are_in2.getTerritory().setId(null);
								else{
									are_in2.getTerritory().setId(Util.getIntegerValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((employee_condition == null || employee_refilter.booleanValue() || employee_condition.evaluate(are_in2.getEmployee()))&&(territory_condition == null || territory_refilter.booleanValue() || territory_condition.evaluate(are_in2.getTerritory())))) {
								if(!(are_in2.getEmployee().equals(new Employee())) && !(are_in2.getTerritory().equals(new Territory())))
									list_res.add(are_in2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(are_in1.getEmployee().equals(new Employee())) && !(are_in1.getTerritory().equals(new Territory())))
							list_res.add(are_in1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Are_in.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
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
			// Role 'employee' mapped to EmbeddedObject 'territories' - 'Territory' containing 'Employee'
			territory_refilter = new MutableBoolean(false);
			res_are_in_employee = are_inService.getAre_inListInmongoSchemaEmployeesterritories(employee_condition, territory_condition, employee_refilter, territory_refilter);
		 	
			datasetsPOJO.add(res_are_in_employee);
			
			
			//Join datasets or return 
			Dataset<Are_in> res = fullOuterJoinsAre_in(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employee> lonelyEmployee = null;
			Dataset<Territory> lonelyTerritory = null;
			
		
		
			
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
		insertAre_inInEmbeddedStructEmployeesInMyMongoDB(are_in);
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	public 	boolean insertAre_inInEmbeddedStructEmployeesInMyMongoDB(Are_in are_in){
	 	// Rel 'are_in' Insert in embedded structure 'Employees'
	
		Employee employee = are_in.getEmployee();
		Territory territory = are_in.getTerritory();
		Bson filter= new Document();
		Bson updateOp;
		String addToSet;
		List<String> fieldName= new ArrayList();
		List<Bson> arrayFilterCond = new ArrayList();
		Document docterritories_1 = new Document();
		docterritories_1.append("TerritoryDescription",territory.getDescription());
		docterritories_1.append("TerritoryID",territory.getId());
		// field 'region' is mapped to mandatory role 'territory' with opposite role of type 'Region'
				Region regionContains = territory._getRegion();
			if(regionContains==null ){
				logger.error("Physical Structure contains embedded attributes or reference to an indirectly linked object. Please set role attribute 'region' of type 'Region' in entity object 'Territory");
				throw new PhysicalStructureException("Physical Structure contains embedded attributes or reference to an indirectly linked object. Please set role attribute 'region' of type 'Region' in entity object 'Territory");
			}
				Region region = regionContains;
				Document docregion_2 = new Document();
				docregion_2.append("RegionDescription",region.getDescription());
				docregion_2.append("RegionID",region.getId());
				
				docterritories_1.append("region", docregion_2);
		
		// level 1 ascending
		 
		filter = eq("EmployeeID",employee.getId());
		updateOp = addToSet("territories", docterritories_1);
		DBConnectionMgr.update(filter, updateOp, "Employees", "myMongoDB");					
		return true;
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
