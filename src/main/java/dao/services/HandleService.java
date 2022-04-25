package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Handle;
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


public abstract class HandleService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HandleService.class);
	
	
	// Left side 'EmployeeRef' of reference [employeeRef ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInEmployeeRefInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'EmployeeID' of reference [employeeRef ]
	public abstract Dataset<EmployeeTDO> getEmployeeTDOListEmployeeInEmployeeRefInOrdersFromMongoSchema(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Handle> fullLeftOuterJoinBetweenHandleAndEmployee(Dataset<Handle> d1, Dataset<Employee> d2) {
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
		return d2_.map((MapFunction<Row, Handle>) r -> {
				Handle res = new Handle();
	
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
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.id': " + employee.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.id': " + employee.getId() + " and " + id + "." );
				}
				if(id != null)
					employee.setId(id);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (employee.getAddress() != null && address != null && !employee.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.address': " + employee.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.address': " + employee.getAddress() + " and " + address + "." );
				}
				if(address != null)
					employee.setAddress(address);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (employee.getBirthDate() != null && birthDate != null && !employee.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.birthDate': " + employee.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.birthDate': " + employee.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					employee.setBirthDate(birthDate);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (employee.getCity() != null && city != null && !employee.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.city': " + employee.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.city': " + employee.getCity() + " and " + city + "." );
				}
				if(city != null)
					employee.setCity(city);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (employee.getCountry() != null && country != null && !employee.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.country': " + employee.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.country': " + employee.getCountry() + " and " + country + "." );
				}
				if(country != null)
					employee.setCountry(country);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (employee.getExtension() != null && extension != null && !employee.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.extension': " + employee.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.extension': " + employee.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					employee.setExtension(extension);
				String firstname = Util.getStringValue(r.getAs("A_firstname"));
				if (employee.getFirstname() != null && firstname != null && !employee.getFirstname().equals(firstname)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.firstname': " + employee.getFirstname() + " and " + firstname + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.firstname': " + employee.getFirstname() + " and " + firstname + "." );
				}
				if(firstname != null)
					employee.setFirstname(firstname);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (employee.getHireDate() != null && hireDate != null && !employee.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.hireDate': " + employee.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.hireDate': " + employee.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					employee.setHireDate(hireDate);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (employee.getHomePhone() != null && homePhone != null && !employee.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.homePhone': " + employee.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.homePhone': " + employee.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					employee.setHomePhone(homePhone);
				String lastname = Util.getStringValue(r.getAs("A_lastname"));
				if (employee.getLastname() != null && lastname != null && !employee.getLastname().equals(lastname)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.lastname': " + employee.getLastname() + " and " + lastname + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.lastname': " + employee.getLastname() + " and " + lastname + "." );
				}
				if(lastname != null)
					employee.setLastname(lastname);
				String photo = Util.getStringValue(r.getAs("A_photo"));
				if (employee.getPhoto() != null && photo != null && !employee.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.photo': " + employee.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.photo': " + employee.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					employee.setPhoto(photo);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (employee.getPostalCode() != null && postalCode != null && !employee.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.postalCode': " + employee.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.postalCode': " + employee.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					employee.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (employee.getRegion() != null && region != null && !employee.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.region': " + employee.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.region': " + employee.getRegion() + " and " + region + "." );
				}
				if(region != null)
					employee.setRegion(region);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (employee.getSalary() != null && salary != null && !employee.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.salary': " + employee.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.salary': " + employee.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					employee.setSalary(salary);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (employee.getTitle() != null && title != null && !employee.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.title': " + employee.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.title': " + employee.getTitle() + " and " + title + "." );
				}
				if(title != null)
					employee.setTitle(title);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (employee.getNotes() != null && notes != null && !employee.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.notes': " + employee.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.notes': " + employee.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					employee.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (employee.getPhotoPath() != null && photoPath != null && !employee.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.photoPath': " + employee.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.photoPath': " + employee.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					employee.setPhotoPath(photoPath);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (employee.getTitleOfCourtesy() != null && titleOfCourtesy != null && !employee.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.titleOfCourtesy': " + employee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.titleOfCourtesy': " + employee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					employee.setTitleOfCourtesy(titleOfCourtesy);
	
				o = r.getAs("order");
				Order order = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getIntegerValue(r2.getAs("id")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				return res;
		}, Encoders.bean(Handle.class));
	
		
		
	}
	public static Dataset<Handle> fullLeftOuterJoinBetweenHandleAndOrder(Dataset<Handle> d1, Dataset<Order> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("freight", "A_freight")
			.withColumnRenamed("orderDate", "A_orderDate")
			.withColumnRenamed("requiredDate", "A_requiredDate")
			.withColumnRenamed("shipAddress", "A_shipAddress")
			.withColumnRenamed("shipCity", "A_shipCity")
			.withColumnRenamed("shipCountry", "A_shipCountry")
			.withColumnRenamed("shipName", "A_shipName")
			.withColumnRenamed("shipPostalCode", "A_shipPostalCode")
			.withColumnRenamed("shipRegion", "A_shipRegion")
			.withColumnRenamed("shippedDate", "A_shippedDate")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("order.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Handle>) r -> {
				Handle res = new Handle();
	
				Order order = new Order();
				Object o = r.getAs("order");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getIntegerValue(r2.getAs("id")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (order.getId() != null && id != null && !order.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.id': " + order.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.id': " + order.getId() + " and " + id + "." );
				}
				if(id != null)
					order.setId(id);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (order.getFreight() != null && freight != null && !order.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.freight': " + order.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.freight': " + order.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					order.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (order.getOrderDate() != null && orderDate != null && !order.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					order.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (order.getRequiredDate() != null && requiredDate != null && !order.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					order.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (order.getShipAddress() != null && shipAddress != null && !order.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					order.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (order.getShipCity() != null && shipCity != null && !order.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					order.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (order.getShipCountry() != null && shipCountry != null && !order.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					order.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (order.getShipName() != null && shipName != null && !order.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shipName': " + order.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shipName': " + order.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					order.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (order.getShipPostalCode() != null && shipPostalCode != null && !order.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					order.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (order.getShipRegion() != null && shipRegion != null && !order.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					order.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (order.getShippedDate() != null && shippedDate != null && !order.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Handle - different values found for attribute 'Handle.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Handle - different values found for attribute 'Handle.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					order.setShippedDate(shippedDate);
	
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
		}, Encoders.bean(Handle.class));
	
		
		
	}
	
	public static Dataset<Handle> fullOuterJoinsHandle(List<Dataset<Handle>> datasetsPOJO) {
		return fullOuterJoinsHandle(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Handle> fullLeftOuterJoinsHandle(List<Dataset<Handle>> datasetsPOJO) {
		return fullOuterJoinsHandle(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Handle> fullOuterJoinsHandle(List<Dataset<Handle>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("employee.id");
	
		idFields.add("order.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Handle> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("employee_id_" + i, d.col("employee.id"))
				.withColumn("order_id_" + i, d.col("order.id"))
				.withColumnRenamed("employee", "employee_" + i)
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("employee_id_0").equalTo(rows.get(1).col("employee_id_1"));
		joinCond = joinCond.and(rows.get(0).col("order_id_0").equalTo(rows.get(1).col("order_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("employee_id_" + (i - 1)).equalTo(rows.get(i).col("employee_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("order_id_" + (i - 1)).equalTo(rows.get(i).col("order_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Handle>) r -> {
				Handle handle_res = new Handle();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							handle_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							handle_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Employee employee_res = new Employee();
					Order order_res = new Order();
					
					// attribute 'Employee.id'
					Integer firstNotNull_employee_id = Util.getIntegerValue(r.getAs("employee_0.id"));
					employee_res.setId(firstNotNull_employee_id);
					// attribute 'Employee.address'
					String firstNotNull_employee_address = Util.getStringValue(r.getAs("employee_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employee_address2 = Util.getStringValue(r.getAs("employee_" + i + ".address"));
						if (firstNotNull_employee_address != null && employee_address2 != null && !firstNotNull_employee_address.equals(employee_address2)) {
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_employee_address + " and " + employee_address2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_employee_birthDate + " and " + employee_birthDate2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_employee_city + " and " + employee_city2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_employee_country + " and " + employee_country2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_employee_extension + " and " + employee_extension2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_employee_firstname + " and " + employee_firstname2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_employee_hireDate + " and " + employee_hireDate2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_employee_homePhone + " and " + employee_homePhone2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_employee_lastname + " and " + employee_lastname2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_employee_photo + " and " + employee_photo2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_employee_postalCode + " and " + employee_postalCode2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_employee_region + " and " + employee_region2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_employee_salary + " and " + employee_salary2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_employee_title + " and " + employee_title2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_employee_notes + " and " + employee_notes2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_employee_photoPath + " and " + employee_photoPath2 + "." );
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
							handle_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_employee_titleOfCourtesy + " and " + employee_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_employee_titleOfCourtesy + " and " + employee_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_employee_titleOfCourtesy == null && employee_titleOfCourtesy2 != null) {
							firstNotNull_employee_titleOfCourtesy = employee_titleOfCourtesy2;
						}
					}
					employee_res.setTitleOfCourtesy(firstNotNull_employee_titleOfCourtesy);
					// attribute 'Order.id'
					Integer firstNotNull_order_id = Util.getIntegerValue(r.getAs("order_0.id"));
					order_res.setId(firstNotNull_order_id);
					// attribute 'Order.freight'
					Double firstNotNull_order_freight = Util.getDoubleValue(r.getAs("order_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double order_freight2 = Util.getDoubleValue(r.getAs("order_" + i + ".freight"));
						if (firstNotNull_order_freight != null && order_freight2 != null && !firstNotNull_order_freight.equals(order_freight2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
						}
						if (firstNotNull_order_freight == null && order_freight2 != null) {
							firstNotNull_order_freight = order_freight2;
						}
					}
					order_res.setFreight(firstNotNull_order_freight);
					// attribute 'Order.orderDate'
					LocalDate firstNotNull_order_orderDate = Util.getLocalDateValue(r.getAs("order_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_orderDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".orderDate"));
						if (firstNotNull_order_orderDate != null && order_orderDate2 != null && !firstNotNull_order_orderDate.equals(order_orderDate2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
						}
						if (firstNotNull_order_orderDate == null && order_orderDate2 != null) {
							firstNotNull_order_orderDate = order_orderDate2;
						}
					}
					order_res.setOrderDate(firstNotNull_order_orderDate);
					// attribute 'Order.requiredDate'
					LocalDate firstNotNull_order_requiredDate = Util.getLocalDateValue(r.getAs("order_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_requiredDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".requiredDate"));
						if (firstNotNull_order_requiredDate != null && order_requiredDate2 != null && !firstNotNull_order_requiredDate.equals(order_requiredDate2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
						}
						if (firstNotNull_order_requiredDate == null && order_requiredDate2 != null) {
							firstNotNull_order_requiredDate = order_requiredDate2;
						}
					}
					order_res.setRequiredDate(firstNotNull_order_requiredDate);
					// attribute 'Order.shipAddress'
					String firstNotNull_order_shipAddress = Util.getStringValue(r.getAs("order_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipAddress2 = Util.getStringValue(r.getAs("order_" + i + ".shipAddress"));
						if (firstNotNull_order_shipAddress != null && order_shipAddress2 != null && !firstNotNull_order_shipAddress.equals(order_shipAddress2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
						}
						if (firstNotNull_order_shipAddress == null && order_shipAddress2 != null) {
							firstNotNull_order_shipAddress = order_shipAddress2;
						}
					}
					order_res.setShipAddress(firstNotNull_order_shipAddress);
					// attribute 'Order.shipCity'
					String firstNotNull_order_shipCity = Util.getStringValue(r.getAs("order_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCity2 = Util.getStringValue(r.getAs("order_" + i + ".shipCity"));
						if (firstNotNull_order_shipCity != null && order_shipCity2 != null && !firstNotNull_order_shipCity.equals(order_shipCity2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
						}
						if (firstNotNull_order_shipCity == null && order_shipCity2 != null) {
							firstNotNull_order_shipCity = order_shipCity2;
						}
					}
					order_res.setShipCity(firstNotNull_order_shipCity);
					// attribute 'Order.shipCountry'
					String firstNotNull_order_shipCountry = Util.getStringValue(r.getAs("order_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCountry2 = Util.getStringValue(r.getAs("order_" + i + ".shipCountry"));
						if (firstNotNull_order_shipCountry != null && order_shipCountry2 != null && !firstNotNull_order_shipCountry.equals(order_shipCountry2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
						}
						if (firstNotNull_order_shipCountry == null && order_shipCountry2 != null) {
							firstNotNull_order_shipCountry = order_shipCountry2;
						}
					}
					order_res.setShipCountry(firstNotNull_order_shipCountry);
					// attribute 'Order.shipName'
					String firstNotNull_order_shipName = Util.getStringValue(r.getAs("order_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipName2 = Util.getStringValue(r.getAs("order_" + i + ".shipName"));
						if (firstNotNull_order_shipName != null && order_shipName2 != null && !firstNotNull_order_shipName.equals(order_shipName2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
						}
						if (firstNotNull_order_shipName == null && order_shipName2 != null) {
							firstNotNull_order_shipName = order_shipName2;
						}
					}
					order_res.setShipName(firstNotNull_order_shipName);
					// attribute 'Order.shipPostalCode'
					String firstNotNull_order_shipPostalCode = Util.getStringValue(r.getAs("order_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipPostalCode2 = Util.getStringValue(r.getAs("order_" + i + ".shipPostalCode"));
						if (firstNotNull_order_shipPostalCode != null && order_shipPostalCode2 != null && !firstNotNull_order_shipPostalCode.equals(order_shipPostalCode2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
						}
						if (firstNotNull_order_shipPostalCode == null && order_shipPostalCode2 != null) {
							firstNotNull_order_shipPostalCode = order_shipPostalCode2;
						}
					}
					order_res.setShipPostalCode(firstNotNull_order_shipPostalCode);
					// attribute 'Order.shipRegion'
					String firstNotNull_order_shipRegion = Util.getStringValue(r.getAs("order_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipRegion2 = Util.getStringValue(r.getAs("order_" + i + ".shipRegion"));
						if (firstNotNull_order_shipRegion != null && order_shipRegion2 != null && !firstNotNull_order_shipRegion.equals(order_shipRegion2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
						}
						if (firstNotNull_order_shipRegion == null && order_shipRegion2 != null) {
							firstNotNull_order_shipRegion = order_shipRegion2;
						}
					}
					order_res.setShipRegion(firstNotNull_order_shipRegion);
					// attribute 'Order.shippedDate'
					LocalDate firstNotNull_order_shippedDate = Util.getLocalDateValue(r.getAs("order_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_shippedDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".shippedDate"));
						if (firstNotNull_order_shippedDate != null && order_shippedDate2 != null && !firstNotNull_order_shippedDate.equals(order_shippedDate2)) {
							handle_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
						}
						if (firstNotNull_order_shippedDate == null && order_shippedDate2 != null) {
							firstNotNull_order_shippedDate = order_shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_order_shippedDate);
	
					handle_res.setEmployee(employee_res);
					handle_res.setOrder(order_res);
					return handle_res;
		}
		, Encoders.bean(Handle.class));
	
	}
	
	//Empty arguments
	public Dataset<Handle> getHandleList(){
		 return getHandleList(null,null);
	}
	
	public abstract Dataset<Handle> getHandleList(
		Condition<EmployeeAttribute> employee_condition,
		Condition<OrderAttribute> order_condition);
	
	public Dataset<Handle> getHandleListByEmployeeCondition(
		Condition<EmployeeAttribute> employee_condition
	){
		return getHandleList(employee_condition, null);
	}
	
	public Dataset<Handle> getHandleListByEmployee(Employee employee) {
		Condition<EmployeeAttribute> cond = null;
		cond = Condition.simple(EmployeeAttribute.id, Operator.EQUALS, employee.getId());
		Dataset<Handle> res = getHandleListByEmployeeCondition(cond);
	return res;
	}
	public Dataset<Handle> getHandleListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getHandleList(null, order_condition);
	}
	
	public Handle getHandleByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Handle> res = getHandleListByOrderCondition(cond);
		List<Handle> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	
	
	public abstract void deleteHandleList(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteHandleListByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteHandleList(employee_condition, null);
	}
	
	public void deleteHandleListByEmployee(pojo.Employee employee) {
		// TODO using id for selecting
		return;
	}
	public void deleteHandleListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteHandleList(null, order_condition);
	}
	
	public void deleteHandleByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
