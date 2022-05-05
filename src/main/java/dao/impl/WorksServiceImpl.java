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
import conditions.WorksAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeesTDO;
import tdo.WorksTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
import tdo.TerritoriesTDO;
import tdo.WorksTDO;
import conditions.TerritoriesAttribute;
import dao.services.TerritoriesService;
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

public class WorksServiceImpl extends dao.services.WorksService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorksServiceImpl.class);
	
	// method accessing the embedded object territories mapped to role employed
	public Dataset<Works> getWorksListInmongoDBEmployeesterritories(Condition<EmployeesAttribute> employed_condition, Condition<TerritoriesAttribute> territories_condition, MutableBoolean employed_refilter, MutableBoolean territories_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = EmployeesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(employed_condition ,employed_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = TerritoriesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(territories_condition ,territories_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
		
			Dataset<Works> res = dataset.flatMap((FlatMapFunction<Row, Works>) r -> {
					List<Works> list_res = new ArrayList<Works>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Works works1 = new Works();
					works1.setEmployed(new Employees());
					works1.setTerritories(new Territories());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employees.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID")==null)
							works1.getEmployed().setEmployeeID(null);
						else{
							works1.getEmployed().setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName")==null)
							works1.getEmployed().setLastName(null);
						else{
							works1.getEmployed().setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName")==null)
							works1.getEmployed().setFirstName(null);
						else{
							works1.getEmployed().setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title")==null)
							works1.getEmployed().setTitle(null);
						else{
							works1.getEmployed().setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy")==null)
							works1.getEmployed().setTitleOfCourtesy(null);
						else{
							works1.getEmployed().setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate")==null)
							works1.getEmployed().setBirthDate(null);
						else{
							works1.getEmployed().setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate")==null)
							works1.getEmployed().setHireDate(null);
						else{
							works1.getEmployed().setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone")==null)
							works1.getEmployed().setHomePhone(null);
						else{
							works1.getEmployed().setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension")==null)
							works1.getEmployed().setExtension(null);
						else{
							works1.getEmployed().setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo")==null)
							works1.getEmployed().setPhoto(null);
						else{
							works1.getEmployed().setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes")==null)
							works1.getEmployed().setNotes(null);
						else{
							works1.getEmployed().setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath")==null)
							works1.getEmployed().setPhotoPath(null);
						else{
							works1.getEmployed().setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary")==null)
							works1.getEmployed().setSalary(null);
						else{
							works1.getEmployed().setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address")==null)
							works1.getEmployed().setAddress(null);
						else{
							works1.getEmployed().setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City")==null)
							works1.getEmployed().setCity(null);
						else{
							works1.getEmployed().setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region")==null)
							works1.getEmployed().setRegion(null);
						else{
							works1.getEmployed().setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode")==null)
							works1.getEmployed().setPostalCode(null);
						else{
							works1.getEmployed().setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country")==null)
							works1.getEmployed().setCountry(null);
						else{
							works1.getEmployed().setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("territories");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Works works2 = (Works) works1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Territories.territoryID for field TerritoryID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryID")) {
								if(nestedRow.getAs("TerritoryID")==null)
									works2.getTerritories().setTerritoryID(null);
								else{
									works2.getTerritories().setTerritoryID(Util.getStringValue(nestedRow.getAs("TerritoryID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Territories.territoryDescription for field TerritoryDescription			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TerritoryDescription")) {
								if(nestedRow.getAs("TerritoryDescription")==null)
									works2.getTerritories().setTerritoryDescription(null);
								else{
									works2.getTerritories().setTerritoryDescription(Util.getStringValue(nestedRow.getAs("TerritoryDescription")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((employed_condition == null || employed_refilter.booleanValue() || employed_condition.evaluate(works2.getEmployed()))&&(territories_condition == null || territories_refilter.booleanValue() || territories_condition.evaluate(works2.getTerritories())))) {
								if(!(works2.getEmployed().equals(new Employees())) && !(works2.getTerritories().equals(new Territories())))
									list_res.add(works2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(works1.getEmployed().equals(new Employees())) && !(works1.getTerritories().equals(new Territories())))
							list_res.add(works1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Works.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	public Dataset<Works> getWorksList(
		Condition<EmployeesAttribute> employed_condition,
		Condition<TerritoriesAttribute> territories_condition){
			WorksServiceImpl worksService = this;
			EmployeesService employeesService = new EmployeesServiceImpl();  
			TerritoriesService territoriesService = new TerritoriesServiceImpl();
			MutableBoolean employed_refilter = new MutableBoolean(false);
			List<Dataset<Works>> datasetsPOJO = new ArrayList<Dataset<Works>>();
			boolean all_already_persisted = false;
			MutableBoolean territories_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Works> res_works_employed;
			Dataset<Employees> res_Employees;
			// Role 'employed' mapped to EmbeddedObject 'territories' - 'Territories' containing 'Employees'
			territories_refilter = new MutableBoolean(false);
			res_works_employed = worksService.getWorksListInmongoDBEmployeesterritories(employed_condition, territories_condition, employed_refilter, territories_refilter);
		 	
			datasetsPOJO.add(res_works_employed);
			
			
			//Join datasets or return 
			Dataset<Works> res = fullOuterJoinsWorks(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employees> lonelyEmployed = null;
			Dataset<Territories> lonelyTerritories = null;
			
		
		
			
			if(employed_refilter.booleanValue() || territories_refilter.booleanValue())
				res = res.filter((FilterFunction<Works>) r -> (employed_condition == null || employed_condition.evaluate(r.getEmployed())) && (territories_condition == null || territories_condition.evaluate(r.getTerritories())));
			
		
			return res;
		
		}
	
	public Dataset<Works> getWorksListByEmployedCondition(
		Condition<EmployeesAttribute> employed_condition
	){
		return getWorksList(employed_condition, null);
	}
	
	public Dataset<Works> getWorksListByEmployed(Employees employed) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, employed.getEmployeeID());
		Dataset<Works> res = getWorksListByEmployedCondition(cond);
	return res;
	}
	public Dataset<Works> getWorksListByTerritoriesCondition(
		Condition<TerritoriesAttribute> territories_condition
	){
		return getWorksList(null, territories_condition);
	}
	
	public Dataset<Works> getWorksListByTerritories(Territories territories) {
		Condition<TerritoriesAttribute> cond = null;
		cond = Condition.simple(TerritoriesAttribute.territoryID, Operator.EQUALS, territories.getTerritoryID());
		Dataset<Works> res = getWorksListByTerritoriesCondition(cond);
	return res;
	}
	
	public void insertWorks(Works works){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		insertWorksInEmbeddedStructEmployeesInMyMongoDB(works);
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	public 	boolean insertWorksInEmbeddedStructEmployeesInMyMongoDB(Works works){
	 	// Rel 'works' Insert in embedded structure 'Employees'
	
		Employees employees = works.getEmployed();
		Territories territories = works.getTerritories();
		Bson filter= new Document();
		Bson updateOp;
		String addToSet;
		List<String> fieldName= new ArrayList();
		List<Bson> arrayFilterCond = new ArrayList();
		Document docterritories_1 = new Document();
		docterritories_1.append("TerritoryID",territories.getTerritoryID());
		docterritories_1.append("TerritoryDescription",territories.getTerritoryDescription());
		// field 'region' is mapped to mandatory role 'territories' with opposite role of type 'Region'
				Region regionLocatedIn = territories._getRegion();
			if(regionLocatedIn==null ){
				logger.error("Physical Structure contains embedded attributes or reference to an indirectly linked object. Please set role attribute 'region' of type 'Region' in entity object 'Territories");
				throw new PhysicalStructureException("Physical Structure contains embedded attributes or reference to an indirectly linked object. Please set role attribute 'region' of type 'Region' in entity object 'Territories");
			}
				Region region = regionLocatedIn;
				Document docregion_2 = new Document();
				docregion_2.append("RegionID",region.getRegionID());
				docregion_2.append("RegionDescription",region.getRegionDescription());
				
				docterritories_1.append("region", docregion_2);
		
		// level 1 ascending
		 
		filter = eq("EmployeeID",employees.getEmployeeID());
		updateOp = addToSet("territories", docterritories_1);
		DBConnectionMgr.update(filter, updateOp, "Employees", "myMongoDB");					
		return true;
	}
	
	
	
	
	
	public void deleteWorksList(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
			//TODO
		}
	
	public void deleteWorksListByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteWorksList(employed_condition, null);
	}
	
	public void deleteWorksListByEmployed(pojo.Employees employed) {
		// TODO using id for selecting
		return;
	}
	public void deleteWorksListByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteWorksList(null, territories_condition);
	}
	
	public void deleteWorksListByTerritories(pojo.Territories territories) {
		// TODO using id for selecting
		return;
	}
		
}
