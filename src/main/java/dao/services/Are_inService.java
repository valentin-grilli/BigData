package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Are_in;
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


public abstract class Are_inService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Are_inService.class);
	
	// method accessing the embedded object territories mapped to role employee
	public abstract Dataset<Are_in> getAre_inListInmongoSchemaEmployeesterritories(Condition<EmployeeAttribute> employee_condition, Condition<TerritoryAttribute> territory_condition, MutableBoolean employee_refilter, MutableBoolean territory_refilter);
	
	
	public static Dataset<Are_in> fullLeftOuterJoinBetweenAre_inAndEmployee(Dataset<Are_in> d1, Dataset<Employee> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("birthDate", "A_birthDate")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("extension", "A_extension")
			.withColumnRenamed("firstname", "A_firstname")
			.withColumnRenamed("hireDate", "A_hireDate")
			.withColumnRenamed("homePhone", "A_homePhone")
			.withColumnRenamed("lastname", "A_lastname")
			.withColumnRenamed("photo", "A_photo")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("salary", "A_salary")
			.withColumnRenamed("title", "A_title")
			.withColumnRenamed("notes", "A_notes")
			.withColumnRenamed("photoPath", "A_photoPath")
			.withColumnRenamed("titleOfCourtesy", "A_titleOfCourtesy")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("employee.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Are_in>) r -> {
				Are_in res = new Are_in();
	
				Employee employee = new Employee();
				Object o = r.getAs("employee");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employee.setId(Util.getIntegerValue(r2.getAs("id")));
						employee.setAddress(Util.getStringValue(r2.getAs("address")));
						employee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employee.setCity(Util.getStringValue(r2.getAs("city")));
						employee.setCountry(Util.getStringValue(r2.getAs("country")));
						employee.setExtension(Util.getStringValue(r2.getAs("extension")));
						employee.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						employee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employee.setLastname(Util.getStringValue(r2.getAs("lastname")));
						employee.setPhoto(Util.getStringValue(r2.getAs("photo")));
						employee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employee.setRegion(Util.getStringValue(r2.getAs("region")));
						employee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						employee.setTitle(Util.getStringValue(r2.getAs("title")));
						employee.setNotes(Util.getStringValue(r2.getAs("notes")));
						employee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						employee = (Employee) o;
					}
				}
	
				res.setEmployee(employee);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (employee.getId() != null && id != null && !employee.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.id': " + employee.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.id': " + employee.getId() + " and " + id + "." );
				}
				if(id != null)
					employee.setId(id);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (employee.getAddress() != null && address != null && !employee.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.address': " + employee.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.address': " + employee.getAddress() + " and " + address + "." );
				}
				if(address != null)
					employee.setAddress(address);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (employee.getBirthDate() != null && birthDate != null && !employee.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.birthDate': " + employee.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.birthDate': " + employee.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					employee.setBirthDate(birthDate);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (employee.getCity() != null && city != null && !employee.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.city': " + employee.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.city': " + employee.getCity() + " and " + city + "." );
				}
				if(city != null)
					employee.setCity(city);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (employee.getCountry() != null && country != null && !employee.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.country': " + employee.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.country': " + employee.getCountry() + " and " + country + "." );
				}
				if(country != null)
					employee.setCountry(country);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (employee.getExtension() != null && extension != null && !employee.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.extension': " + employee.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.extension': " + employee.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					employee.setExtension(extension);
				String firstname = Util.getStringValue(r.getAs("A_firstname"));
				if (employee.getFirstname() != null && firstname != null && !employee.getFirstname().equals(firstname)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.firstname': " + employee.getFirstname() + " and " + firstname + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.firstname': " + employee.getFirstname() + " and " + firstname + "." );
				}
				if(firstname != null)
					employee.setFirstname(firstname);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (employee.getHireDate() != null && hireDate != null && !employee.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.hireDate': " + employee.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.hireDate': " + employee.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					employee.setHireDate(hireDate);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (employee.getHomePhone() != null && homePhone != null && !employee.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.homePhone': " + employee.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.homePhone': " + employee.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					employee.setHomePhone(homePhone);
				String lastname = Util.getStringValue(r.getAs("A_lastname"));
				if (employee.getLastname() != null && lastname != null && !employee.getLastname().equals(lastname)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.lastname': " + employee.getLastname() + " and " + lastname + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.lastname': " + employee.getLastname() + " and " + lastname + "." );
				}
				if(lastname != null)
					employee.setLastname(lastname);
				String photo = Util.getStringValue(r.getAs("A_photo"));
				if (employee.getPhoto() != null && photo != null && !employee.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.photo': " + employee.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.photo': " + employee.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					employee.setPhoto(photo);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (employee.getPostalCode() != null && postalCode != null && !employee.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.postalCode': " + employee.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.postalCode': " + employee.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					employee.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (employee.getRegion() != null && region != null && !employee.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.region': " + employee.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.region': " + employee.getRegion() + " and " + region + "." );
				}
				if(region != null)
					employee.setRegion(region);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (employee.getSalary() != null && salary != null && !employee.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.salary': " + employee.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.salary': " + employee.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					employee.setSalary(salary);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (employee.getTitle() != null && title != null && !employee.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.title': " + employee.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.title': " + employee.getTitle() + " and " + title + "." );
				}
				if(title != null)
					employee.setTitle(title);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (employee.getNotes() != null && notes != null && !employee.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.notes': " + employee.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.notes': " + employee.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					employee.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (employee.getPhotoPath() != null && photoPath != null && !employee.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.photoPath': " + employee.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.photoPath': " + employee.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					employee.setPhotoPath(photoPath);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (employee.getTitleOfCourtesy() != null && titleOfCourtesy != null && !employee.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.titleOfCourtesy': " + employee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.titleOfCourtesy': " + employee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					employee.setTitleOfCourtesy(titleOfCourtesy);
	
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
		}, Encoders.bean(Are_in.class));
	
		
		
	}
	public static Dataset<Are_in> fullLeftOuterJoinBetweenAre_inAndTerritory(Dataset<Are_in> d1, Dataset<Territory> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("description", "A_description")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("territory.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Are_in>) r -> {
				Are_in res = new Are_in();
	
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
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.id': " + territory.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.id': " + territory.getId() + " and " + id + "." );
				}
				if(id != null)
					territory.setId(id);
				String description = Util.getStringValue(r.getAs("A_description"));
				if (territory.getDescription() != null && description != null && !territory.getDescription().equals(description)) {
					res.addLogEvent("Data consistency problem for [Are_in - different values found for attribute 'Are_in.description': " + territory.getDescription() + " and " + description + "." );
					logger.warn("Data consistency problem for [Are_in - different values found for attribute 'Are_in.description': " + territory.getDescription() + " and " + description + "." );
				}
				if(description != null)
					territory.setDescription(description);
	
				o = r.getAs("employee");
				Employee employee = new Employee();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employee.setId(Util.getIntegerValue(r2.getAs("id")));
						employee.setAddress(Util.getStringValue(r2.getAs("address")));
						employee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employee.setCity(Util.getStringValue(r2.getAs("city")));
						employee.setCountry(Util.getStringValue(r2.getAs("country")));
						employee.setExtension(Util.getStringValue(r2.getAs("extension")));
						employee.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						employee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employee.setLastname(Util.getStringValue(r2.getAs("lastname")));
						employee.setPhoto(Util.getStringValue(r2.getAs("photo")));
						employee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employee.setRegion(Util.getStringValue(r2.getAs("region")));
						employee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						employee.setTitle(Util.getStringValue(r2.getAs("title")));
						employee.setNotes(Util.getStringValue(r2.getAs("notes")));
						employee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						employee = (Employee) o;
					}
				}
	
				res.setEmployee(employee);
	
				return res;
		}, Encoders.bean(Are_in.class));
	
		
		
	}
	
	public static Dataset<Are_in> fullOuterJoinsAre_in(List<Dataset<Are_in>> datasetsPOJO) {
		return fullOuterJoinsAre_in(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Are_in> fullLeftOuterJoinsAre_in(List<Dataset<Are_in>> datasetsPOJO) {
		return fullOuterJoinsAre_in(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Are_in> fullOuterJoinsAre_in(List<Dataset<Are_in>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("employee.id");
	
		idFields.add("territory.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Are_in> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("employee_id_" + i, d.col("employee.id"))
				.withColumn("territory_id_" + i, d.col("territory.id"))
				.withColumnRenamed("employee", "employee_" + i)
				.withColumnRenamed("territory", "territory_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("employee_id_0").equalTo(rows.get(1).col("employee_id_1"));
		joinCond = joinCond.and(rows.get(0).col("territory_id_0").equalTo(rows.get(1).col("territory_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("employee_id_" + (i - 1)).equalTo(rows.get(i).col("employee_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("territory_id_" + (i - 1)).equalTo(rows.get(i).col("territory_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Are_in>) r -> {
				Are_in are_in_res = new Are_in();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							are_in_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							are_in_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Employee employee_res = new Employee();
					Territory territory_res = new Territory();
					
					// attribute 'Employee.id'
					Integer firstNotNull_employee_id = Util.getIntegerValue(r.getAs("employee_0.id"));
					employee_res.setId(firstNotNull_employee_id);
					// attribute 'Employee.address'
					String firstNotNull_employee_address = Util.getStringValue(r.getAs("employee_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_address2 = Util.getStringValue(r.getAs("employee_" + i + ".address"));
						if (firstNotNull_employee_address != null && employee_address2 != null && !firstNotNull_employee_address.equals(employee_address2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_employee_address + " and " + employee_address2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_employee_address + " and " + employee_address2 + "." );
						}
						if (firstNotNull_employee_address == null && employee_address2 != null) {
							firstNotNull_employee_address = employee_address2;
						}
					}
					employee_res.setAddress(firstNotNull_employee_address);
					// attribute 'Employee.birthDate'
					LocalDate firstNotNull_employee_birthDate = Util.getLocalDateValue(r.getAs("employee_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employee_birthDate2 = Util.getLocalDateValue(r.getAs("employee_" + i + ".birthDate"));
						if (firstNotNull_employee_birthDate != null && employee_birthDate2 != null && !firstNotNull_employee_birthDate.equals(employee_birthDate2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_employee_birthDate + " and " + employee_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_employee_birthDate + " and " + employee_birthDate2 + "." );
						}
						if (firstNotNull_employee_birthDate == null && employee_birthDate2 != null) {
							firstNotNull_employee_birthDate = employee_birthDate2;
						}
					}
					employee_res.setBirthDate(firstNotNull_employee_birthDate);
					// attribute 'Employee.city'
					String firstNotNull_employee_city = Util.getStringValue(r.getAs("employee_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_city2 = Util.getStringValue(r.getAs("employee_" + i + ".city"));
						if (firstNotNull_employee_city != null && employee_city2 != null && !firstNotNull_employee_city.equals(employee_city2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_employee_city + " and " + employee_city2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_employee_city + " and " + employee_city2 + "." );
						}
						if (firstNotNull_employee_city == null && employee_city2 != null) {
							firstNotNull_employee_city = employee_city2;
						}
					}
					employee_res.setCity(firstNotNull_employee_city);
					// attribute 'Employee.country'
					String firstNotNull_employee_country = Util.getStringValue(r.getAs("employee_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_country2 = Util.getStringValue(r.getAs("employee_" + i + ".country"));
						if (firstNotNull_employee_country != null && employee_country2 != null && !firstNotNull_employee_country.equals(employee_country2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_employee_country + " and " + employee_country2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_employee_country + " and " + employee_country2 + "." );
						}
						if (firstNotNull_employee_country == null && employee_country2 != null) {
							firstNotNull_employee_country = employee_country2;
						}
					}
					employee_res.setCountry(firstNotNull_employee_country);
					// attribute 'Employee.extension'
					String firstNotNull_employee_extension = Util.getStringValue(r.getAs("employee_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_extension2 = Util.getStringValue(r.getAs("employee_" + i + ".extension"));
						if (firstNotNull_employee_extension != null && employee_extension2 != null && !firstNotNull_employee_extension.equals(employee_extension2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_employee_extension + " and " + employee_extension2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_employee_extension + " and " + employee_extension2 + "." );
						}
						if (firstNotNull_employee_extension == null && employee_extension2 != null) {
							firstNotNull_employee_extension = employee_extension2;
						}
					}
					employee_res.setExtension(firstNotNull_employee_extension);
					// attribute 'Employee.firstname'
					String firstNotNull_employee_firstname = Util.getStringValue(r.getAs("employee_0.firstname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_firstname2 = Util.getStringValue(r.getAs("employee_" + i + ".firstname"));
						if (firstNotNull_employee_firstname != null && employee_firstname2 != null && !firstNotNull_employee_firstname.equals(employee_firstname2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_employee_firstname + " and " + employee_firstname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_employee_firstname + " and " + employee_firstname2 + "." );
						}
						if (firstNotNull_employee_firstname == null && employee_firstname2 != null) {
							firstNotNull_employee_firstname = employee_firstname2;
						}
					}
					employee_res.setFirstname(firstNotNull_employee_firstname);
					// attribute 'Employee.hireDate'
					LocalDate firstNotNull_employee_hireDate = Util.getLocalDateValue(r.getAs("employee_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employee_hireDate2 = Util.getLocalDateValue(r.getAs("employee_" + i + ".hireDate"));
						if (firstNotNull_employee_hireDate != null && employee_hireDate2 != null && !firstNotNull_employee_hireDate.equals(employee_hireDate2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_employee_hireDate + " and " + employee_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_employee_hireDate + " and " + employee_hireDate2 + "." );
						}
						if (firstNotNull_employee_hireDate == null && employee_hireDate2 != null) {
							firstNotNull_employee_hireDate = employee_hireDate2;
						}
					}
					employee_res.setHireDate(firstNotNull_employee_hireDate);
					// attribute 'Employee.homePhone'
					String firstNotNull_employee_homePhone = Util.getStringValue(r.getAs("employee_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_homePhone2 = Util.getStringValue(r.getAs("employee_" + i + ".homePhone"));
						if (firstNotNull_employee_homePhone != null && employee_homePhone2 != null && !firstNotNull_employee_homePhone.equals(employee_homePhone2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_employee_homePhone + " and " + employee_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_employee_homePhone + " and " + employee_homePhone2 + "." );
						}
						if (firstNotNull_employee_homePhone == null && employee_homePhone2 != null) {
							firstNotNull_employee_homePhone = employee_homePhone2;
						}
					}
					employee_res.setHomePhone(firstNotNull_employee_homePhone);
					// attribute 'Employee.lastname'
					String firstNotNull_employee_lastname = Util.getStringValue(r.getAs("employee_0.lastname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_lastname2 = Util.getStringValue(r.getAs("employee_" + i + ".lastname"));
						if (firstNotNull_employee_lastname != null && employee_lastname2 != null && !firstNotNull_employee_lastname.equals(employee_lastname2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_employee_lastname + " and " + employee_lastname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_employee_lastname + " and " + employee_lastname2 + "." );
						}
						if (firstNotNull_employee_lastname == null && employee_lastname2 != null) {
							firstNotNull_employee_lastname = employee_lastname2;
						}
					}
					employee_res.setLastname(firstNotNull_employee_lastname);
					// attribute 'Employee.photo'
					String firstNotNull_employee_photo = Util.getStringValue(r.getAs("employee_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_photo2 = Util.getStringValue(r.getAs("employee_" + i + ".photo"));
						if (firstNotNull_employee_photo != null && employee_photo2 != null && !firstNotNull_employee_photo.equals(employee_photo2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_employee_photo + " and " + employee_photo2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_employee_photo + " and " + employee_photo2 + "." );
						}
						if (firstNotNull_employee_photo == null && employee_photo2 != null) {
							firstNotNull_employee_photo = employee_photo2;
						}
					}
					employee_res.setPhoto(firstNotNull_employee_photo);
					// attribute 'Employee.postalCode'
					String firstNotNull_employee_postalCode = Util.getStringValue(r.getAs("employee_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_postalCode2 = Util.getStringValue(r.getAs("employee_" + i + ".postalCode"));
						if (firstNotNull_employee_postalCode != null && employee_postalCode2 != null && !firstNotNull_employee_postalCode.equals(employee_postalCode2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_employee_postalCode + " and " + employee_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_employee_postalCode + " and " + employee_postalCode2 + "." );
						}
						if (firstNotNull_employee_postalCode == null && employee_postalCode2 != null) {
							firstNotNull_employee_postalCode = employee_postalCode2;
						}
					}
					employee_res.setPostalCode(firstNotNull_employee_postalCode);
					// attribute 'Employee.region'
					String firstNotNull_employee_region = Util.getStringValue(r.getAs("employee_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_region2 = Util.getStringValue(r.getAs("employee_" + i + ".region"));
						if (firstNotNull_employee_region != null && employee_region2 != null && !firstNotNull_employee_region.equals(employee_region2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_employee_region + " and " + employee_region2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_employee_region + " and " + employee_region2 + "." );
						}
						if (firstNotNull_employee_region == null && employee_region2 != null) {
							firstNotNull_employee_region = employee_region2;
						}
					}
					employee_res.setRegion(firstNotNull_employee_region);
					// attribute 'Employee.salary'
					Double firstNotNull_employee_salary = Util.getDoubleValue(r.getAs("employee_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double employee_salary2 = Util.getDoubleValue(r.getAs("employee_" + i + ".salary"));
						if (firstNotNull_employee_salary != null && employee_salary2 != null && !firstNotNull_employee_salary.equals(employee_salary2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_employee_salary + " and " + employee_salary2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_employee_salary + " and " + employee_salary2 + "." );
						}
						if (firstNotNull_employee_salary == null && employee_salary2 != null) {
							firstNotNull_employee_salary = employee_salary2;
						}
					}
					employee_res.setSalary(firstNotNull_employee_salary);
					// attribute 'Employee.title'
					String firstNotNull_employee_title = Util.getStringValue(r.getAs("employee_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_title2 = Util.getStringValue(r.getAs("employee_" + i + ".title"));
						if (firstNotNull_employee_title != null && employee_title2 != null && !firstNotNull_employee_title.equals(employee_title2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_employee_title + " and " + employee_title2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_employee_title + " and " + employee_title2 + "." );
						}
						if (firstNotNull_employee_title == null && employee_title2 != null) {
							firstNotNull_employee_title = employee_title2;
						}
					}
					employee_res.setTitle(firstNotNull_employee_title);
					// attribute 'Employee.notes'
					String firstNotNull_employee_notes = Util.getStringValue(r.getAs("employee_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_notes2 = Util.getStringValue(r.getAs("employee_" + i + ".notes"));
						if (firstNotNull_employee_notes != null && employee_notes2 != null && !firstNotNull_employee_notes.equals(employee_notes2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_employee_notes + " and " + employee_notes2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_employee_notes + " and " + employee_notes2 + "." );
						}
						if (firstNotNull_employee_notes == null && employee_notes2 != null) {
							firstNotNull_employee_notes = employee_notes2;
						}
					}
					employee_res.setNotes(firstNotNull_employee_notes);
					// attribute 'Employee.photoPath'
					String firstNotNull_employee_photoPath = Util.getStringValue(r.getAs("employee_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_photoPath2 = Util.getStringValue(r.getAs("employee_" + i + ".photoPath"));
						if (firstNotNull_employee_photoPath != null && employee_photoPath2 != null && !firstNotNull_employee_photoPath.equals(employee_photoPath2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_employee_photoPath + " and " + employee_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_employee_photoPath + " and " + employee_photoPath2 + "." );
						}
						if (firstNotNull_employee_photoPath == null && employee_photoPath2 != null) {
							firstNotNull_employee_photoPath = employee_photoPath2;
						}
					}
					employee_res.setPhotoPath(firstNotNull_employee_photoPath);
					// attribute 'Employee.titleOfCourtesy'
					String firstNotNull_employee_titleOfCourtesy = Util.getStringValue(r.getAs("employee_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_titleOfCourtesy2 = Util.getStringValue(r.getAs("employee_" + i + ".titleOfCourtesy"));
						if (firstNotNull_employee_titleOfCourtesy != null && employee_titleOfCourtesy2 != null && !firstNotNull_employee_titleOfCourtesy.equals(employee_titleOfCourtesy2)) {
							are_in_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_employee_titleOfCourtesy + " and " + employee_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_employee_titleOfCourtesy + " and " + employee_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_employee_titleOfCourtesy == null && employee_titleOfCourtesy2 != null) {
							firstNotNull_employee_titleOfCourtesy = employee_titleOfCourtesy2;
						}
					}
					employee_res.setTitleOfCourtesy(firstNotNull_employee_titleOfCourtesy);
					// attribute 'Territory.id'
					Integer firstNotNull_territory_id = Util.getIntegerValue(r.getAs("territory_0.id"));
					territory_res.setId(firstNotNull_territory_id);
					// attribute 'Territory.description'
					String firstNotNull_territory_description = Util.getStringValue(r.getAs("territory_0.description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String territory_description2 = Util.getStringValue(r.getAs("territory_" + i + ".description"));
						if (firstNotNull_territory_description != null && territory_description2 != null && !firstNotNull_territory_description.equals(territory_description2)) {
							are_in_res.addLogEvent("Data consistency problem for [Territory - id :"+territory_res.getId()+"]: different values found for attribute 'Territory.description': " + firstNotNull_territory_description + " and " + territory_description2 + "." );
							logger.warn("Data consistency problem for [Territory - id :"+territory_res.getId()+"]: different values found for attribute 'Territory.description': " + firstNotNull_territory_description + " and " + territory_description2 + "." );
						}
						if (firstNotNull_territory_description == null && territory_description2 != null) {
							firstNotNull_territory_description = territory_description2;
						}
					}
					territory_res.setDescription(firstNotNull_territory_description);
	
					are_in_res.setEmployee(employee_res);
					are_in_res.setTerritory(territory_res);
					return are_in_res;
		}
		, Encoders.bean(Are_in.class));
	
	}
	
	//Empty arguments
	public Dataset<Are_in> getAre_inList(){
		 return getAre_inList(null,null);
	}
	
	public abstract Dataset<Are_in> getAre_inList(
		Condition<EmployeeAttribute> employee_condition,
		Condition<TerritoryAttribute> territory_condition);
	
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
	
	public abstract void insertAre_in(Are_in are_in);
	
	
	public 	abstract boolean insertAre_inInEmbeddedStructEmployeesInMyMongoDB(Are_in are_in);
	
	
	 public void insertAre_in(Employee employee ,Territory territory ){
		Are_in are_in = new Are_in();
		are_in.setEmployee(employee);
		are_in.setTerritory(territory);
		insertAre_in(are_in);
	}
	
	 public void insertAre_in(Territory territory, List<Employee> employeeList){
		Are_in are_in = new Are_in();
		are_in.setTerritory(territory);
		for(Employee employee : employeeList){
			are_in.setEmployee(employee);
			insertAre_in(are_in);
		}
	}
	 public void insertAre_in(Employee employee, List<Territory> territoryList){
		Are_in are_in = new Are_in();
		are_in.setEmployee(employee);
		for(Territory territory : territoryList){
			are_in.setTerritory(territory);
			insertAre_in(are_in);
		}
	}
	
	
	public abstract void deleteAre_inList(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.TerritoryAttribute> territory_condition);
	
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
