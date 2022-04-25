package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Report_to;
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


public abstract class Report_toService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Report_toService.class);
	
	
	// Left side 'ReportsTo' of reference [reportToRef ]
	public abstract Dataset<EmployeeTDO> getEmployeeTDOListLowerEmployeeInReportToRefInEmployeesFromMongoSchema(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'EmployeeID' of reference [reportToRef ]
	public abstract Dataset<EmployeeTDO> getEmployeeTDOListHigherEmployeeInReportToRefInEmployeesFromMongoSchema(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Report_to> fullLeftOuterJoinBetweenReport_toAndLowerEmployee(Dataset<Report_to> d1, Dataset<Employee> d2) {
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
		joinCond = d1.col("lowerEmployee.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Report_to>) r -> {
				Report_to res = new Report_to();
	
				Employee lowerEmployee = new Employee();
				Object o = r.getAs("lowerEmployee");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						lowerEmployee.setId(Util.getIntegerValue(r2.getAs("id")));
						lowerEmployee.setAddress(Util.getStringValue(r2.getAs("address")));
						lowerEmployee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						lowerEmployee.setCity(Util.getStringValue(r2.getAs("city")));
						lowerEmployee.setCountry(Util.getStringValue(r2.getAs("country")));
						lowerEmployee.setExtension(Util.getStringValue(r2.getAs("extension")));
						lowerEmployee.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						lowerEmployee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						lowerEmployee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						lowerEmployee.setLastname(Util.getStringValue(r2.getAs("lastname")));
						lowerEmployee.setPhoto(Util.getStringValue(r2.getAs("photo")));
						lowerEmployee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						lowerEmployee.setRegion(Util.getStringValue(r2.getAs("region")));
						lowerEmployee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						lowerEmployee.setTitle(Util.getStringValue(r2.getAs("title")));
						lowerEmployee.setNotes(Util.getStringValue(r2.getAs("notes")));
						lowerEmployee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						lowerEmployee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						lowerEmployee = (Employee) o;
					}
				}
	
				res.setLowerEmployee(lowerEmployee);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (lowerEmployee.getId() != null && id != null && !lowerEmployee.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.id': " + lowerEmployee.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.id': " + lowerEmployee.getId() + " and " + id + "." );
				}
				if(id != null)
					lowerEmployee.setId(id);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (lowerEmployee.getAddress() != null && address != null && !lowerEmployee.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.address': " + lowerEmployee.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.address': " + lowerEmployee.getAddress() + " and " + address + "." );
				}
				if(address != null)
					lowerEmployee.setAddress(address);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (lowerEmployee.getBirthDate() != null && birthDate != null && !lowerEmployee.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.birthDate': " + lowerEmployee.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.birthDate': " + lowerEmployee.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					lowerEmployee.setBirthDate(birthDate);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (lowerEmployee.getCity() != null && city != null && !lowerEmployee.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.city': " + lowerEmployee.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.city': " + lowerEmployee.getCity() + " and " + city + "." );
				}
				if(city != null)
					lowerEmployee.setCity(city);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (lowerEmployee.getCountry() != null && country != null && !lowerEmployee.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.country': " + lowerEmployee.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.country': " + lowerEmployee.getCountry() + " and " + country + "." );
				}
				if(country != null)
					lowerEmployee.setCountry(country);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (lowerEmployee.getExtension() != null && extension != null && !lowerEmployee.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.extension': " + lowerEmployee.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.extension': " + lowerEmployee.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					lowerEmployee.setExtension(extension);
				String firstname = Util.getStringValue(r.getAs("A_firstname"));
				if (lowerEmployee.getFirstname() != null && firstname != null && !lowerEmployee.getFirstname().equals(firstname)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.firstname': " + lowerEmployee.getFirstname() + " and " + firstname + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.firstname': " + lowerEmployee.getFirstname() + " and " + firstname + "." );
				}
				if(firstname != null)
					lowerEmployee.setFirstname(firstname);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (lowerEmployee.getHireDate() != null && hireDate != null && !lowerEmployee.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.hireDate': " + lowerEmployee.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.hireDate': " + lowerEmployee.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					lowerEmployee.setHireDate(hireDate);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (lowerEmployee.getHomePhone() != null && homePhone != null && !lowerEmployee.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.homePhone': " + lowerEmployee.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.homePhone': " + lowerEmployee.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					lowerEmployee.setHomePhone(homePhone);
				String lastname = Util.getStringValue(r.getAs("A_lastname"));
				if (lowerEmployee.getLastname() != null && lastname != null && !lowerEmployee.getLastname().equals(lastname)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.lastname': " + lowerEmployee.getLastname() + " and " + lastname + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.lastname': " + lowerEmployee.getLastname() + " and " + lastname + "." );
				}
				if(lastname != null)
					lowerEmployee.setLastname(lastname);
				String photo = Util.getStringValue(r.getAs("A_photo"));
				if (lowerEmployee.getPhoto() != null && photo != null && !lowerEmployee.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photo': " + lowerEmployee.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photo': " + lowerEmployee.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					lowerEmployee.setPhoto(photo);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (lowerEmployee.getPostalCode() != null && postalCode != null && !lowerEmployee.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.postalCode': " + lowerEmployee.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.postalCode': " + lowerEmployee.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					lowerEmployee.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (lowerEmployee.getRegion() != null && region != null && !lowerEmployee.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.region': " + lowerEmployee.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.region': " + lowerEmployee.getRegion() + " and " + region + "." );
				}
				if(region != null)
					lowerEmployee.setRegion(region);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (lowerEmployee.getSalary() != null && salary != null && !lowerEmployee.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.salary': " + lowerEmployee.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.salary': " + lowerEmployee.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					lowerEmployee.setSalary(salary);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (lowerEmployee.getTitle() != null && title != null && !lowerEmployee.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.title': " + lowerEmployee.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.title': " + lowerEmployee.getTitle() + " and " + title + "." );
				}
				if(title != null)
					lowerEmployee.setTitle(title);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (lowerEmployee.getNotes() != null && notes != null && !lowerEmployee.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.notes': " + lowerEmployee.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.notes': " + lowerEmployee.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					lowerEmployee.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (lowerEmployee.getPhotoPath() != null && photoPath != null && !lowerEmployee.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photoPath': " + lowerEmployee.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photoPath': " + lowerEmployee.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					lowerEmployee.setPhotoPath(photoPath);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (lowerEmployee.getTitleOfCourtesy() != null && titleOfCourtesy != null && !lowerEmployee.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.titleOfCourtesy': " + lowerEmployee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.titleOfCourtesy': " + lowerEmployee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					lowerEmployee.setTitleOfCourtesy(titleOfCourtesy);
	
				o = r.getAs("higherEmployee");
				Employee higherEmployee = new Employee();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						higherEmployee.setId(Util.getIntegerValue(r2.getAs("id")));
						higherEmployee.setAddress(Util.getStringValue(r2.getAs("address")));
						higherEmployee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						higherEmployee.setCity(Util.getStringValue(r2.getAs("city")));
						higherEmployee.setCountry(Util.getStringValue(r2.getAs("country")));
						higherEmployee.setExtension(Util.getStringValue(r2.getAs("extension")));
						higherEmployee.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						higherEmployee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						higherEmployee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						higherEmployee.setLastname(Util.getStringValue(r2.getAs("lastname")));
						higherEmployee.setPhoto(Util.getStringValue(r2.getAs("photo")));
						higherEmployee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						higherEmployee.setRegion(Util.getStringValue(r2.getAs("region")));
						higherEmployee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						higherEmployee.setTitle(Util.getStringValue(r2.getAs("title")));
						higherEmployee.setNotes(Util.getStringValue(r2.getAs("notes")));
						higherEmployee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						higherEmployee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						higherEmployee = (Employee) o;
					}
				}
	
				res.setHigherEmployee(higherEmployee);
	
				return res;
		}, Encoders.bean(Report_to.class));
	
		
		
	}
	public static Dataset<Report_to> fullLeftOuterJoinBetweenReport_toAndHigherEmployee(Dataset<Report_to> d1, Dataset<Employee> d2) {
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
		joinCond = d1.col("higherEmployee.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Report_to>) r -> {
				Report_to res = new Report_to();
	
				Employee higherEmployee = new Employee();
				Object o = r.getAs("higherEmployee");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						higherEmployee.setId(Util.getIntegerValue(r2.getAs("id")));
						higherEmployee.setAddress(Util.getStringValue(r2.getAs("address")));
						higherEmployee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						higherEmployee.setCity(Util.getStringValue(r2.getAs("city")));
						higherEmployee.setCountry(Util.getStringValue(r2.getAs("country")));
						higherEmployee.setExtension(Util.getStringValue(r2.getAs("extension")));
						higherEmployee.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						higherEmployee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						higherEmployee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						higherEmployee.setLastname(Util.getStringValue(r2.getAs("lastname")));
						higherEmployee.setPhoto(Util.getStringValue(r2.getAs("photo")));
						higherEmployee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						higherEmployee.setRegion(Util.getStringValue(r2.getAs("region")));
						higherEmployee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						higherEmployee.setTitle(Util.getStringValue(r2.getAs("title")));
						higherEmployee.setNotes(Util.getStringValue(r2.getAs("notes")));
						higherEmployee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						higherEmployee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						higherEmployee = (Employee) o;
					}
				}
	
				res.setHigherEmployee(higherEmployee);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (higherEmployee.getId() != null && id != null && !higherEmployee.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.id': " + higherEmployee.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.id': " + higherEmployee.getId() + " and " + id + "." );
				}
				if(id != null)
					higherEmployee.setId(id);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (higherEmployee.getAddress() != null && address != null && !higherEmployee.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.address': " + higherEmployee.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.address': " + higherEmployee.getAddress() + " and " + address + "." );
				}
				if(address != null)
					higherEmployee.setAddress(address);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (higherEmployee.getBirthDate() != null && birthDate != null && !higherEmployee.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.birthDate': " + higherEmployee.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.birthDate': " + higherEmployee.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					higherEmployee.setBirthDate(birthDate);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (higherEmployee.getCity() != null && city != null && !higherEmployee.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.city': " + higherEmployee.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.city': " + higherEmployee.getCity() + " and " + city + "." );
				}
				if(city != null)
					higherEmployee.setCity(city);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (higherEmployee.getCountry() != null && country != null && !higherEmployee.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.country': " + higherEmployee.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.country': " + higherEmployee.getCountry() + " and " + country + "." );
				}
				if(country != null)
					higherEmployee.setCountry(country);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (higherEmployee.getExtension() != null && extension != null && !higherEmployee.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.extension': " + higherEmployee.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.extension': " + higherEmployee.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					higherEmployee.setExtension(extension);
				String firstname = Util.getStringValue(r.getAs("A_firstname"));
				if (higherEmployee.getFirstname() != null && firstname != null && !higherEmployee.getFirstname().equals(firstname)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.firstname': " + higherEmployee.getFirstname() + " and " + firstname + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.firstname': " + higherEmployee.getFirstname() + " and " + firstname + "." );
				}
				if(firstname != null)
					higherEmployee.setFirstname(firstname);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (higherEmployee.getHireDate() != null && hireDate != null && !higherEmployee.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.hireDate': " + higherEmployee.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.hireDate': " + higherEmployee.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					higherEmployee.setHireDate(hireDate);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (higherEmployee.getHomePhone() != null && homePhone != null && !higherEmployee.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.homePhone': " + higherEmployee.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.homePhone': " + higherEmployee.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					higherEmployee.setHomePhone(homePhone);
				String lastname = Util.getStringValue(r.getAs("A_lastname"));
				if (higherEmployee.getLastname() != null && lastname != null && !higherEmployee.getLastname().equals(lastname)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.lastname': " + higherEmployee.getLastname() + " and " + lastname + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.lastname': " + higherEmployee.getLastname() + " and " + lastname + "." );
				}
				if(lastname != null)
					higherEmployee.setLastname(lastname);
				String photo = Util.getStringValue(r.getAs("A_photo"));
				if (higherEmployee.getPhoto() != null && photo != null && !higherEmployee.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photo': " + higherEmployee.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photo': " + higherEmployee.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					higherEmployee.setPhoto(photo);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (higherEmployee.getPostalCode() != null && postalCode != null && !higherEmployee.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.postalCode': " + higherEmployee.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.postalCode': " + higherEmployee.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					higherEmployee.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (higherEmployee.getRegion() != null && region != null && !higherEmployee.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.region': " + higherEmployee.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.region': " + higherEmployee.getRegion() + " and " + region + "." );
				}
				if(region != null)
					higherEmployee.setRegion(region);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (higherEmployee.getSalary() != null && salary != null && !higherEmployee.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.salary': " + higherEmployee.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.salary': " + higherEmployee.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					higherEmployee.setSalary(salary);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (higherEmployee.getTitle() != null && title != null && !higherEmployee.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.title': " + higherEmployee.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.title': " + higherEmployee.getTitle() + " and " + title + "." );
				}
				if(title != null)
					higherEmployee.setTitle(title);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (higherEmployee.getNotes() != null && notes != null && !higherEmployee.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.notes': " + higherEmployee.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.notes': " + higherEmployee.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					higherEmployee.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (higherEmployee.getPhotoPath() != null && photoPath != null && !higherEmployee.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photoPath': " + higherEmployee.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.photoPath': " + higherEmployee.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					higherEmployee.setPhotoPath(photoPath);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (higherEmployee.getTitleOfCourtesy() != null && titleOfCourtesy != null && !higherEmployee.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Report_to - different values found for attribute 'Report_to.titleOfCourtesy': " + higherEmployee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Report_to - different values found for attribute 'Report_to.titleOfCourtesy': " + higherEmployee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					higherEmployee.setTitleOfCourtesy(titleOfCourtesy);
	
				o = r.getAs("lowerEmployee");
				Employee lowerEmployee = new Employee();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						lowerEmployee.setId(Util.getIntegerValue(r2.getAs("id")));
						lowerEmployee.setAddress(Util.getStringValue(r2.getAs("address")));
						lowerEmployee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						lowerEmployee.setCity(Util.getStringValue(r2.getAs("city")));
						lowerEmployee.setCountry(Util.getStringValue(r2.getAs("country")));
						lowerEmployee.setExtension(Util.getStringValue(r2.getAs("extension")));
						lowerEmployee.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						lowerEmployee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						lowerEmployee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						lowerEmployee.setLastname(Util.getStringValue(r2.getAs("lastname")));
						lowerEmployee.setPhoto(Util.getStringValue(r2.getAs("photo")));
						lowerEmployee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						lowerEmployee.setRegion(Util.getStringValue(r2.getAs("region")));
						lowerEmployee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						lowerEmployee.setTitle(Util.getStringValue(r2.getAs("title")));
						lowerEmployee.setNotes(Util.getStringValue(r2.getAs("notes")));
						lowerEmployee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						lowerEmployee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						lowerEmployee = (Employee) o;
					}
				}
	
				res.setLowerEmployee(lowerEmployee);
	
				return res;
		}, Encoders.bean(Report_to.class));
	
		
		
	}
	
	public static Dataset<Report_to> fullOuterJoinsReport_to(List<Dataset<Report_to>> datasetsPOJO) {
		return fullOuterJoinsReport_to(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Report_to> fullLeftOuterJoinsReport_to(List<Dataset<Report_to>> datasetsPOJO) {
		return fullOuterJoinsReport_to(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Report_to> fullOuterJoinsReport_to(List<Dataset<Report_to>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("lowerEmployee.id");
	
		idFields.add("higherEmployee.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Report_to> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("lowerEmployee_id_" + i, d.col("lowerEmployee.id"))
				.withColumn("higherEmployee_id_" + i, d.col("higherEmployee.id"))
				.withColumnRenamed("lowerEmployee", "lowerEmployee_" + i)
				.withColumnRenamed("higherEmployee", "higherEmployee_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("lowerEmployee_id_0").equalTo(rows.get(1).col("lowerEmployee_id_1"));
		joinCond = joinCond.and(rows.get(0).col("higherEmployee_id_0").equalTo(rows.get(1).col("higherEmployee_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("lowerEmployee_id_" + (i - 1)).equalTo(rows.get(i).col("lowerEmployee_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("higherEmployee_id_" + (i - 1)).equalTo(rows.get(i).col("higherEmployee_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Report_to>) r -> {
				Report_to report_to_res = new Report_to();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							report_to_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							report_to_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Employee lowerEmployee_res = new Employee();
					Employee higherEmployee_res = new Employee();
					
					// attribute 'Employee.id'
					Integer firstNotNull_lowerEmployee_id = Util.getIntegerValue(r.getAs("lowerEmployee_0.id"));
					lowerEmployee_res.setId(firstNotNull_lowerEmployee_id);
					// attribute 'Employee.address'
					String firstNotNull_lowerEmployee_address = Util.getStringValue(r.getAs("lowerEmployee_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_address2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".address"));
						if (firstNotNull_lowerEmployee_address != null && lowerEmployee_address2 != null && !firstNotNull_lowerEmployee_address.equals(lowerEmployee_address2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_lowerEmployee_address + " and " + lowerEmployee_address2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_lowerEmployee_address + " and " + lowerEmployee_address2 + "." );
						}
						if (firstNotNull_lowerEmployee_address == null && lowerEmployee_address2 != null) {
							firstNotNull_lowerEmployee_address = lowerEmployee_address2;
						}
					}
					lowerEmployee_res.setAddress(firstNotNull_lowerEmployee_address);
					// attribute 'Employee.birthDate'
					LocalDate firstNotNull_lowerEmployee_birthDate = Util.getLocalDateValue(r.getAs("lowerEmployee_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate lowerEmployee_birthDate2 = Util.getLocalDateValue(r.getAs("lowerEmployee_" + i + ".birthDate"));
						if (firstNotNull_lowerEmployee_birthDate != null && lowerEmployee_birthDate2 != null && !firstNotNull_lowerEmployee_birthDate.equals(lowerEmployee_birthDate2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_lowerEmployee_birthDate + " and " + lowerEmployee_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_lowerEmployee_birthDate + " and " + lowerEmployee_birthDate2 + "." );
						}
						if (firstNotNull_lowerEmployee_birthDate == null && lowerEmployee_birthDate2 != null) {
							firstNotNull_lowerEmployee_birthDate = lowerEmployee_birthDate2;
						}
					}
					lowerEmployee_res.setBirthDate(firstNotNull_lowerEmployee_birthDate);
					// attribute 'Employee.city'
					String firstNotNull_lowerEmployee_city = Util.getStringValue(r.getAs("lowerEmployee_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_city2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".city"));
						if (firstNotNull_lowerEmployee_city != null && lowerEmployee_city2 != null && !firstNotNull_lowerEmployee_city.equals(lowerEmployee_city2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_lowerEmployee_city + " and " + lowerEmployee_city2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_lowerEmployee_city + " and " + lowerEmployee_city2 + "." );
						}
						if (firstNotNull_lowerEmployee_city == null && lowerEmployee_city2 != null) {
							firstNotNull_lowerEmployee_city = lowerEmployee_city2;
						}
					}
					lowerEmployee_res.setCity(firstNotNull_lowerEmployee_city);
					// attribute 'Employee.country'
					String firstNotNull_lowerEmployee_country = Util.getStringValue(r.getAs("lowerEmployee_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_country2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".country"));
						if (firstNotNull_lowerEmployee_country != null && lowerEmployee_country2 != null && !firstNotNull_lowerEmployee_country.equals(lowerEmployee_country2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_lowerEmployee_country + " and " + lowerEmployee_country2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_lowerEmployee_country + " and " + lowerEmployee_country2 + "." );
						}
						if (firstNotNull_lowerEmployee_country == null && lowerEmployee_country2 != null) {
							firstNotNull_lowerEmployee_country = lowerEmployee_country2;
						}
					}
					lowerEmployee_res.setCountry(firstNotNull_lowerEmployee_country);
					// attribute 'Employee.extension'
					String firstNotNull_lowerEmployee_extension = Util.getStringValue(r.getAs("lowerEmployee_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_extension2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".extension"));
						if (firstNotNull_lowerEmployee_extension != null && lowerEmployee_extension2 != null && !firstNotNull_lowerEmployee_extension.equals(lowerEmployee_extension2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_lowerEmployee_extension + " and " + lowerEmployee_extension2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_lowerEmployee_extension + " and " + lowerEmployee_extension2 + "." );
						}
						if (firstNotNull_lowerEmployee_extension == null && lowerEmployee_extension2 != null) {
							firstNotNull_lowerEmployee_extension = lowerEmployee_extension2;
						}
					}
					lowerEmployee_res.setExtension(firstNotNull_lowerEmployee_extension);
					// attribute 'Employee.firstname'
					String firstNotNull_lowerEmployee_firstname = Util.getStringValue(r.getAs("lowerEmployee_0.firstname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_firstname2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".firstname"));
						if (firstNotNull_lowerEmployee_firstname != null && lowerEmployee_firstname2 != null && !firstNotNull_lowerEmployee_firstname.equals(lowerEmployee_firstname2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_lowerEmployee_firstname + " and " + lowerEmployee_firstname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_lowerEmployee_firstname + " and " + lowerEmployee_firstname2 + "." );
						}
						if (firstNotNull_lowerEmployee_firstname == null && lowerEmployee_firstname2 != null) {
							firstNotNull_lowerEmployee_firstname = lowerEmployee_firstname2;
						}
					}
					lowerEmployee_res.setFirstname(firstNotNull_lowerEmployee_firstname);
					// attribute 'Employee.hireDate'
					LocalDate firstNotNull_lowerEmployee_hireDate = Util.getLocalDateValue(r.getAs("lowerEmployee_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate lowerEmployee_hireDate2 = Util.getLocalDateValue(r.getAs("lowerEmployee_" + i + ".hireDate"));
						if (firstNotNull_lowerEmployee_hireDate != null && lowerEmployee_hireDate2 != null && !firstNotNull_lowerEmployee_hireDate.equals(lowerEmployee_hireDate2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_lowerEmployee_hireDate + " and " + lowerEmployee_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_lowerEmployee_hireDate + " and " + lowerEmployee_hireDate2 + "." );
						}
						if (firstNotNull_lowerEmployee_hireDate == null && lowerEmployee_hireDate2 != null) {
							firstNotNull_lowerEmployee_hireDate = lowerEmployee_hireDate2;
						}
					}
					lowerEmployee_res.setHireDate(firstNotNull_lowerEmployee_hireDate);
					// attribute 'Employee.homePhone'
					String firstNotNull_lowerEmployee_homePhone = Util.getStringValue(r.getAs("lowerEmployee_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_homePhone2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".homePhone"));
						if (firstNotNull_lowerEmployee_homePhone != null && lowerEmployee_homePhone2 != null && !firstNotNull_lowerEmployee_homePhone.equals(lowerEmployee_homePhone2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_lowerEmployee_homePhone + " and " + lowerEmployee_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_lowerEmployee_homePhone + " and " + lowerEmployee_homePhone2 + "." );
						}
						if (firstNotNull_lowerEmployee_homePhone == null && lowerEmployee_homePhone2 != null) {
							firstNotNull_lowerEmployee_homePhone = lowerEmployee_homePhone2;
						}
					}
					lowerEmployee_res.setHomePhone(firstNotNull_lowerEmployee_homePhone);
					// attribute 'Employee.lastname'
					String firstNotNull_lowerEmployee_lastname = Util.getStringValue(r.getAs("lowerEmployee_0.lastname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_lastname2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".lastname"));
						if (firstNotNull_lowerEmployee_lastname != null && lowerEmployee_lastname2 != null && !firstNotNull_lowerEmployee_lastname.equals(lowerEmployee_lastname2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_lowerEmployee_lastname + " and " + lowerEmployee_lastname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_lowerEmployee_lastname + " and " + lowerEmployee_lastname2 + "." );
						}
						if (firstNotNull_lowerEmployee_lastname == null && lowerEmployee_lastname2 != null) {
							firstNotNull_lowerEmployee_lastname = lowerEmployee_lastname2;
						}
					}
					lowerEmployee_res.setLastname(firstNotNull_lowerEmployee_lastname);
					// attribute 'Employee.photo'
					String firstNotNull_lowerEmployee_photo = Util.getStringValue(r.getAs("lowerEmployee_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_photo2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".photo"));
						if (firstNotNull_lowerEmployee_photo != null && lowerEmployee_photo2 != null && !firstNotNull_lowerEmployee_photo.equals(lowerEmployee_photo2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_lowerEmployee_photo + " and " + lowerEmployee_photo2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_lowerEmployee_photo + " and " + lowerEmployee_photo2 + "." );
						}
						if (firstNotNull_lowerEmployee_photo == null && lowerEmployee_photo2 != null) {
							firstNotNull_lowerEmployee_photo = lowerEmployee_photo2;
						}
					}
					lowerEmployee_res.setPhoto(firstNotNull_lowerEmployee_photo);
					// attribute 'Employee.postalCode'
					String firstNotNull_lowerEmployee_postalCode = Util.getStringValue(r.getAs("lowerEmployee_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_postalCode2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".postalCode"));
						if (firstNotNull_lowerEmployee_postalCode != null && lowerEmployee_postalCode2 != null && !firstNotNull_lowerEmployee_postalCode.equals(lowerEmployee_postalCode2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_lowerEmployee_postalCode + " and " + lowerEmployee_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_lowerEmployee_postalCode + " and " + lowerEmployee_postalCode2 + "." );
						}
						if (firstNotNull_lowerEmployee_postalCode == null && lowerEmployee_postalCode2 != null) {
							firstNotNull_lowerEmployee_postalCode = lowerEmployee_postalCode2;
						}
					}
					lowerEmployee_res.setPostalCode(firstNotNull_lowerEmployee_postalCode);
					// attribute 'Employee.region'
					String firstNotNull_lowerEmployee_region = Util.getStringValue(r.getAs("lowerEmployee_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_region2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".region"));
						if (firstNotNull_lowerEmployee_region != null && lowerEmployee_region2 != null && !firstNotNull_lowerEmployee_region.equals(lowerEmployee_region2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_lowerEmployee_region + " and " + lowerEmployee_region2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_lowerEmployee_region + " and " + lowerEmployee_region2 + "." );
						}
						if (firstNotNull_lowerEmployee_region == null && lowerEmployee_region2 != null) {
							firstNotNull_lowerEmployee_region = lowerEmployee_region2;
						}
					}
					lowerEmployee_res.setRegion(firstNotNull_lowerEmployee_region);
					// attribute 'Employee.salary'
					Double firstNotNull_lowerEmployee_salary = Util.getDoubleValue(r.getAs("lowerEmployee_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double lowerEmployee_salary2 = Util.getDoubleValue(r.getAs("lowerEmployee_" + i + ".salary"));
						if (firstNotNull_lowerEmployee_salary != null && lowerEmployee_salary2 != null && !firstNotNull_lowerEmployee_salary.equals(lowerEmployee_salary2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_lowerEmployee_salary + " and " + lowerEmployee_salary2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_lowerEmployee_salary + " and " + lowerEmployee_salary2 + "." );
						}
						if (firstNotNull_lowerEmployee_salary == null && lowerEmployee_salary2 != null) {
							firstNotNull_lowerEmployee_salary = lowerEmployee_salary2;
						}
					}
					lowerEmployee_res.setSalary(firstNotNull_lowerEmployee_salary);
					// attribute 'Employee.title'
					String firstNotNull_lowerEmployee_title = Util.getStringValue(r.getAs("lowerEmployee_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_title2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".title"));
						if (firstNotNull_lowerEmployee_title != null && lowerEmployee_title2 != null && !firstNotNull_lowerEmployee_title.equals(lowerEmployee_title2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_lowerEmployee_title + " and " + lowerEmployee_title2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_lowerEmployee_title + " and " + lowerEmployee_title2 + "." );
						}
						if (firstNotNull_lowerEmployee_title == null && lowerEmployee_title2 != null) {
							firstNotNull_lowerEmployee_title = lowerEmployee_title2;
						}
					}
					lowerEmployee_res.setTitle(firstNotNull_lowerEmployee_title);
					// attribute 'Employee.notes'
					String firstNotNull_lowerEmployee_notes = Util.getStringValue(r.getAs("lowerEmployee_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_notes2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".notes"));
						if (firstNotNull_lowerEmployee_notes != null && lowerEmployee_notes2 != null && !firstNotNull_lowerEmployee_notes.equals(lowerEmployee_notes2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_lowerEmployee_notes + " and " + lowerEmployee_notes2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_lowerEmployee_notes + " and " + lowerEmployee_notes2 + "." );
						}
						if (firstNotNull_lowerEmployee_notes == null && lowerEmployee_notes2 != null) {
							firstNotNull_lowerEmployee_notes = lowerEmployee_notes2;
						}
					}
					lowerEmployee_res.setNotes(firstNotNull_lowerEmployee_notes);
					// attribute 'Employee.photoPath'
					String firstNotNull_lowerEmployee_photoPath = Util.getStringValue(r.getAs("lowerEmployee_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_photoPath2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".photoPath"));
						if (firstNotNull_lowerEmployee_photoPath != null && lowerEmployee_photoPath2 != null && !firstNotNull_lowerEmployee_photoPath.equals(lowerEmployee_photoPath2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_lowerEmployee_photoPath + " and " + lowerEmployee_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_lowerEmployee_photoPath + " and " + lowerEmployee_photoPath2 + "." );
						}
						if (firstNotNull_lowerEmployee_photoPath == null && lowerEmployee_photoPath2 != null) {
							firstNotNull_lowerEmployee_photoPath = lowerEmployee_photoPath2;
						}
					}
					lowerEmployee_res.setPhotoPath(firstNotNull_lowerEmployee_photoPath);
					// attribute 'Employee.titleOfCourtesy'
					String firstNotNull_lowerEmployee_titleOfCourtesy = Util.getStringValue(r.getAs("lowerEmployee_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lowerEmployee_titleOfCourtesy2 = Util.getStringValue(r.getAs("lowerEmployee_" + i + ".titleOfCourtesy"));
						if (firstNotNull_lowerEmployee_titleOfCourtesy != null && lowerEmployee_titleOfCourtesy2 != null && !firstNotNull_lowerEmployee_titleOfCourtesy.equals(lowerEmployee_titleOfCourtesy2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_lowerEmployee_titleOfCourtesy + " and " + lowerEmployee_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+lowerEmployee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_lowerEmployee_titleOfCourtesy + " and " + lowerEmployee_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_lowerEmployee_titleOfCourtesy == null && lowerEmployee_titleOfCourtesy2 != null) {
							firstNotNull_lowerEmployee_titleOfCourtesy = lowerEmployee_titleOfCourtesy2;
						}
					}
					lowerEmployee_res.setTitleOfCourtesy(firstNotNull_lowerEmployee_titleOfCourtesy);
					// attribute 'Employee.id'
					Integer firstNotNull_higherEmployee_id = Util.getIntegerValue(r.getAs("higherEmployee_0.id"));
					higherEmployee_res.setId(firstNotNull_higherEmployee_id);
					// attribute 'Employee.address'
					String firstNotNull_higherEmployee_address = Util.getStringValue(r.getAs("higherEmployee_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_address2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".address"));
						if (firstNotNull_higherEmployee_address != null && higherEmployee_address2 != null && !firstNotNull_higherEmployee_address.equals(higherEmployee_address2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_higherEmployee_address + " and " + higherEmployee_address2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_higherEmployee_address + " and " + higherEmployee_address2 + "." );
						}
						if (firstNotNull_higherEmployee_address == null && higherEmployee_address2 != null) {
							firstNotNull_higherEmployee_address = higherEmployee_address2;
						}
					}
					higherEmployee_res.setAddress(firstNotNull_higherEmployee_address);
					// attribute 'Employee.birthDate'
					LocalDate firstNotNull_higherEmployee_birthDate = Util.getLocalDateValue(r.getAs("higherEmployee_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate higherEmployee_birthDate2 = Util.getLocalDateValue(r.getAs("higherEmployee_" + i + ".birthDate"));
						if (firstNotNull_higherEmployee_birthDate != null && higherEmployee_birthDate2 != null && !firstNotNull_higherEmployee_birthDate.equals(higherEmployee_birthDate2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_higherEmployee_birthDate + " and " + higherEmployee_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_higherEmployee_birthDate + " and " + higherEmployee_birthDate2 + "." );
						}
						if (firstNotNull_higherEmployee_birthDate == null && higherEmployee_birthDate2 != null) {
							firstNotNull_higherEmployee_birthDate = higherEmployee_birthDate2;
						}
					}
					higherEmployee_res.setBirthDate(firstNotNull_higherEmployee_birthDate);
					// attribute 'Employee.city'
					String firstNotNull_higherEmployee_city = Util.getStringValue(r.getAs("higherEmployee_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_city2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".city"));
						if (firstNotNull_higherEmployee_city != null && higherEmployee_city2 != null && !firstNotNull_higherEmployee_city.equals(higherEmployee_city2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_higherEmployee_city + " and " + higherEmployee_city2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_higherEmployee_city + " and " + higherEmployee_city2 + "." );
						}
						if (firstNotNull_higherEmployee_city == null && higherEmployee_city2 != null) {
							firstNotNull_higherEmployee_city = higherEmployee_city2;
						}
					}
					higherEmployee_res.setCity(firstNotNull_higherEmployee_city);
					// attribute 'Employee.country'
					String firstNotNull_higherEmployee_country = Util.getStringValue(r.getAs("higherEmployee_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_country2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".country"));
						if (firstNotNull_higherEmployee_country != null && higherEmployee_country2 != null && !firstNotNull_higherEmployee_country.equals(higherEmployee_country2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_higherEmployee_country + " and " + higherEmployee_country2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_higherEmployee_country + " and " + higherEmployee_country2 + "." );
						}
						if (firstNotNull_higherEmployee_country == null && higherEmployee_country2 != null) {
							firstNotNull_higherEmployee_country = higherEmployee_country2;
						}
					}
					higherEmployee_res.setCountry(firstNotNull_higherEmployee_country);
					// attribute 'Employee.extension'
					String firstNotNull_higherEmployee_extension = Util.getStringValue(r.getAs("higherEmployee_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_extension2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".extension"));
						if (firstNotNull_higherEmployee_extension != null && higherEmployee_extension2 != null && !firstNotNull_higherEmployee_extension.equals(higherEmployee_extension2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_higherEmployee_extension + " and " + higherEmployee_extension2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_higherEmployee_extension + " and " + higherEmployee_extension2 + "." );
						}
						if (firstNotNull_higherEmployee_extension == null && higherEmployee_extension2 != null) {
							firstNotNull_higherEmployee_extension = higherEmployee_extension2;
						}
					}
					higherEmployee_res.setExtension(firstNotNull_higherEmployee_extension);
					// attribute 'Employee.firstname'
					String firstNotNull_higherEmployee_firstname = Util.getStringValue(r.getAs("higherEmployee_0.firstname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_firstname2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".firstname"));
						if (firstNotNull_higherEmployee_firstname != null && higherEmployee_firstname2 != null && !firstNotNull_higherEmployee_firstname.equals(higherEmployee_firstname2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_higherEmployee_firstname + " and " + higherEmployee_firstname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_higherEmployee_firstname + " and " + higherEmployee_firstname2 + "." );
						}
						if (firstNotNull_higherEmployee_firstname == null && higherEmployee_firstname2 != null) {
							firstNotNull_higherEmployee_firstname = higherEmployee_firstname2;
						}
					}
					higherEmployee_res.setFirstname(firstNotNull_higherEmployee_firstname);
					// attribute 'Employee.hireDate'
					LocalDate firstNotNull_higherEmployee_hireDate = Util.getLocalDateValue(r.getAs("higherEmployee_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate higherEmployee_hireDate2 = Util.getLocalDateValue(r.getAs("higherEmployee_" + i + ".hireDate"));
						if (firstNotNull_higherEmployee_hireDate != null && higherEmployee_hireDate2 != null && !firstNotNull_higherEmployee_hireDate.equals(higherEmployee_hireDate2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_higherEmployee_hireDate + " and " + higherEmployee_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_higherEmployee_hireDate + " and " + higherEmployee_hireDate2 + "." );
						}
						if (firstNotNull_higherEmployee_hireDate == null && higherEmployee_hireDate2 != null) {
							firstNotNull_higherEmployee_hireDate = higherEmployee_hireDate2;
						}
					}
					higherEmployee_res.setHireDate(firstNotNull_higherEmployee_hireDate);
					// attribute 'Employee.homePhone'
					String firstNotNull_higherEmployee_homePhone = Util.getStringValue(r.getAs("higherEmployee_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_homePhone2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".homePhone"));
						if (firstNotNull_higherEmployee_homePhone != null && higherEmployee_homePhone2 != null && !firstNotNull_higherEmployee_homePhone.equals(higherEmployee_homePhone2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_higherEmployee_homePhone + " and " + higherEmployee_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_higherEmployee_homePhone + " and " + higherEmployee_homePhone2 + "." );
						}
						if (firstNotNull_higherEmployee_homePhone == null && higherEmployee_homePhone2 != null) {
							firstNotNull_higherEmployee_homePhone = higherEmployee_homePhone2;
						}
					}
					higherEmployee_res.setHomePhone(firstNotNull_higherEmployee_homePhone);
					// attribute 'Employee.lastname'
					String firstNotNull_higherEmployee_lastname = Util.getStringValue(r.getAs("higherEmployee_0.lastname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_lastname2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".lastname"));
						if (firstNotNull_higherEmployee_lastname != null && higherEmployee_lastname2 != null && !firstNotNull_higherEmployee_lastname.equals(higherEmployee_lastname2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_higherEmployee_lastname + " and " + higherEmployee_lastname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_higherEmployee_lastname + " and " + higherEmployee_lastname2 + "." );
						}
						if (firstNotNull_higherEmployee_lastname == null && higherEmployee_lastname2 != null) {
							firstNotNull_higherEmployee_lastname = higherEmployee_lastname2;
						}
					}
					higherEmployee_res.setLastname(firstNotNull_higherEmployee_lastname);
					// attribute 'Employee.photo'
					String firstNotNull_higherEmployee_photo = Util.getStringValue(r.getAs("higherEmployee_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_photo2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".photo"));
						if (firstNotNull_higherEmployee_photo != null && higherEmployee_photo2 != null && !firstNotNull_higherEmployee_photo.equals(higherEmployee_photo2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_higherEmployee_photo + " and " + higherEmployee_photo2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_higherEmployee_photo + " and " + higherEmployee_photo2 + "." );
						}
						if (firstNotNull_higherEmployee_photo == null && higherEmployee_photo2 != null) {
							firstNotNull_higherEmployee_photo = higherEmployee_photo2;
						}
					}
					higherEmployee_res.setPhoto(firstNotNull_higherEmployee_photo);
					// attribute 'Employee.postalCode'
					String firstNotNull_higherEmployee_postalCode = Util.getStringValue(r.getAs("higherEmployee_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_postalCode2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".postalCode"));
						if (firstNotNull_higherEmployee_postalCode != null && higherEmployee_postalCode2 != null && !firstNotNull_higherEmployee_postalCode.equals(higherEmployee_postalCode2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_higherEmployee_postalCode + " and " + higherEmployee_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_higherEmployee_postalCode + " and " + higherEmployee_postalCode2 + "." );
						}
						if (firstNotNull_higherEmployee_postalCode == null && higherEmployee_postalCode2 != null) {
							firstNotNull_higherEmployee_postalCode = higherEmployee_postalCode2;
						}
					}
					higherEmployee_res.setPostalCode(firstNotNull_higherEmployee_postalCode);
					// attribute 'Employee.region'
					String firstNotNull_higherEmployee_region = Util.getStringValue(r.getAs("higherEmployee_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_region2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".region"));
						if (firstNotNull_higherEmployee_region != null && higherEmployee_region2 != null && !firstNotNull_higherEmployee_region.equals(higherEmployee_region2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_higherEmployee_region + " and " + higherEmployee_region2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_higherEmployee_region + " and " + higherEmployee_region2 + "." );
						}
						if (firstNotNull_higherEmployee_region == null && higherEmployee_region2 != null) {
							firstNotNull_higherEmployee_region = higherEmployee_region2;
						}
					}
					higherEmployee_res.setRegion(firstNotNull_higherEmployee_region);
					// attribute 'Employee.salary'
					Double firstNotNull_higherEmployee_salary = Util.getDoubleValue(r.getAs("higherEmployee_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double higherEmployee_salary2 = Util.getDoubleValue(r.getAs("higherEmployee_" + i + ".salary"));
						if (firstNotNull_higherEmployee_salary != null && higherEmployee_salary2 != null && !firstNotNull_higherEmployee_salary.equals(higherEmployee_salary2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_higherEmployee_salary + " and " + higherEmployee_salary2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_higherEmployee_salary + " and " + higherEmployee_salary2 + "." );
						}
						if (firstNotNull_higherEmployee_salary == null && higherEmployee_salary2 != null) {
							firstNotNull_higherEmployee_salary = higherEmployee_salary2;
						}
					}
					higherEmployee_res.setSalary(firstNotNull_higherEmployee_salary);
					// attribute 'Employee.title'
					String firstNotNull_higherEmployee_title = Util.getStringValue(r.getAs("higherEmployee_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_title2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".title"));
						if (firstNotNull_higherEmployee_title != null && higherEmployee_title2 != null && !firstNotNull_higherEmployee_title.equals(higherEmployee_title2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_higherEmployee_title + " and " + higherEmployee_title2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_higherEmployee_title + " and " + higherEmployee_title2 + "." );
						}
						if (firstNotNull_higherEmployee_title == null && higherEmployee_title2 != null) {
							firstNotNull_higherEmployee_title = higherEmployee_title2;
						}
					}
					higherEmployee_res.setTitle(firstNotNull_higherEmployee_title);
					// attribute 'Employee.notes'
					String firstNotNull_higherEmployee_notes = Util.getStringValue(r.getAs("higherEmployee_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_notes2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".notes"));
						if (firstNotNull_higherEmployee_notes != null && higherEmployee_notes2 != null && !firstNotNull_higherEmployee_notes.equals(higherEmployee_notes2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_higherEmployee_notes + " and " + higherEmployee_notes2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_higherEmployee_notes + " and " + higherEmployee_notes2 + "." );
						}
						if (firstNotNull_higherEmployee_notes == null && higherEmployee_notes2 != null) {
							firstNotNull_higherEmployee_notes = higherEmployee_notes2;
						}
					}
					higherEmployee_res.setNotes(firstNotNull_higherEmployee_notes);
					// attribute 'Employee.photoPath'
					String firstNotNull_higherEmployee_photoPath = Util.getStringValue(r.getAs("higherEmployee_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_photoPath2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".photoPath"));
						if (firstNotNull_higherEmployee_photoPath != null && higherEmployee_photoPath2 != null && !firstNotNull_higherEmployee_photoPath.equals(higherEmployee_photoPath2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_higherEmployee_photoPath + " and " + higherEmployee_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_higherEmployee_photoPath + " and " + higherEmployee_photoPath2 + "." );
						}
						if (firstNotNull_higherEmployee_photoPath == null && higherEmployee_photoPath2 != null) {
							firstNotNull_higherEmployee_photoPath = higherEmployee_photoPath2;
						}
					}
					higherEmployee_res.setPhotoPath(firstNotNull_higherEmployee_photoPath);
					// attribute 'Employee.titleOfCourtesy'
					String firstNotNull_higherEmployee_titleOfCourtesy = Util.getStringValue(r.getAs("higherEmployee_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String higherEmployee_titleOfCourtesy2 = Util.getStringValue(r.getAs("higherEmployee_" + i + ".titleOfCourtesy"));
						if (firstNotNull_higherEmployee_titleOfCourtesy != null && higherEmployee_titleOfCourtesy2 != null && !firstNotNull_higherEmployee_titleOfCourtesy.equals(higherEmployee_titleOfCourtesy2)) {
							report_to_res.addLogEvent("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_higherEmployee_titleOfCourtesy + " and " + higherEmployee_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+higherEmployee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_higherEmployee_titleOfCourtesy + " and " + higherEmployee_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_higherEmployee_titleOfCourtesy == null && higherEmployee_titleOfCourtesy2 != null) {
							firstNotNull_higherEmployee_titleOfCourtesy = higherEmployee_titleOfCourtesy2;
						}
					}
					higherEmployee_res.setTitleOfCourtesy(firstNotNull_higherEmployee_titleOfCourtesy);
	
					report_to_res.setLowerEmployee(lowerEmployee_res);
					report_to_res.setHigherEmployee(higherEmployee_res);
					return report_to_res;
		}
		, Encoders.bean(Report_to.class));
	
	}
	
	//Empty arguments
	public Dataset<Report_to> getReport_toList(){
		 return getReport_toList(null,null);
	}
	
	public abstract Dataset<Report_to> getReport_toList(
		Condition<EmployeeAttribute> lowerEmployee_condition,
		Condition<EmployeeAttribute> higherEmployee_condition);
	
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
	
	public abstract void insertReport_to(Report_to report_to);
	
	
	
	public 	abstract boolean insertReport_toInRefStructEmployeesInMyMongoDB(Report_to report_to);
	
	 public void insertReport_to(Employee lowerEmployee ,Employee higherEmployee ){
		Report_to report_to = new Report_to();
		report_to.setLowerEmployee(lowerEmployee);
		report_to.setHigherEmployee(higherEmployee);
		insertReport_to(report_to);
	}
	
	 public void insertReport_to(Employee employee, List<Employee> higherEmployeeList){
		Report_to report_to = new Report_to();
		report_to.setLowerEmployee(employee);
		for(Employee higherEmployee : higherEmployeeList){
			report_to.setHigherEmployee(higherEmployee);
			insertReport_to(report_to);
		}
	}
	
	
	public abstract void deleteReport_toList(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition);
	
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
