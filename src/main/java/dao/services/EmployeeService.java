package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Employee;
import conditions.EmployeeAttribute;
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
import conditions.EmployeeAttribute;
import pojo.Are_in;
import conditions.TerritoryAttribute;
import pojo.Territory;
import conditions.EmployeeAttribute;
import pojo.Report_to;
import conditions.EmployeeAttribute;
import pojo.Employee;
import conditions.EmployeeAttribute;
import pojo.Report_to;
import conditions.EmployeeAttribute;
import pojo.Employee;
import conditions.EmployeeAttribute;
import pojo.Handle;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class EmployeeService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeeService.class);
	protected Are_inService are_inService = new dao.impl.Are_inServiceImpl();
	protected Report_toService report_toService = new dao.impl.Report_toServiceImpl();
	protected HandleService handleService = new dao.impl.HandleServiceImpl();
	


	public static enum ROLE_NAME {
		ARE_IN_EMPLOYEE, REPORT_TO_LOWEREMPLOYEE, REPORT_TO_HIGHEREMPLOYEE, HANDLE_EMPLOYEE
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.ARE_IN_EMPLOYEE, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.REPORT_TO_LOWEREMPLOYEE, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.REPORT_TO_HIGHEREMPLOYEE, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.HANDLE_EMPLOYEE, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public EmployeeService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public EmployeeService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Employee> getEmployeeList(){
		return getEmployeeList(null);
	}
	
	public Dataset<Employee> getEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Employee>> datasets = new ArrayList<Dataset<Employee>>();
		Dataset<Employee> d = null;
		d = getEmployeeListInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsEmployee(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Employee>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Employee> getEmployeeListInEmployeesFromMyMongoDB(conditions.Condition<conditions.EmployeeAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Employee getEmployeeById(Integer id){
		Condition cond;
		cond = Condition.simple(EmployeeAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Employee> res = getEmployeeList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Employee> getEmployeeListById(Integer id) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Employee> getEmployeeListByAddress(String address) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	public Dataset<Employee> getEmployeeListByBirthDate(LocalDate birthDate) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.birthDate, conditions.Operator.EQUALS, birthDate));
	}
	
	public Dataset<Employee> getEmployeeListByCity(String city) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	public Dataset<Employee> getEmployeeListByCountry(String country) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.country, conditions.Operator.EQUALS, country));
	}
	
	public Dataset<Employee> getEmployeeListByExtension(String extension) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.extension, conditions.Operator.EQUALS, extension));
	}
	
	public Dataset<Employee> getEmployeeListByFirstname(String firstname) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.firstname, conditions.Operator.EQUALS, firstname));
	}
	
	public Dataset<Employee> getEmployeeListByHireDate(LocalDate hireDate) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.hireDate, conditions.Operator.EQUALS, hireDate));
	}
	
	public Dataset<Employee> getEmployeeListByHomePhone(String homePhone) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.homePhone, conditions.Operator.EQUALS, homePhone));
	}
	
	public Dataset<Employee> getEmployeeListByLastname(String lastname) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.lastname, conditions.Operator.EQUALS, lastname));
	}
	
	public Dataset<Employee> getEmployeeListByPhoto(String photo) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.photo, conditions.Operator.EQUALS, photo));
	}
	
	public Dataset<Employee> getEmployeeListByPostalCode(String postalCode) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.postalCode, conditions.Operator.EQUALS, postalCode));
	}
	
	public Dataset<Employee> getEmployeeListByRegion(String region) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.region, conditions.Operator.EQUALS, region));
	}
	
	public Dataset<Employee> getEmployeeListBySalary(Double salary) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.salary, conditions.Operator.EQUALS, salary));
	}
	
	public Dataset<Employee> getEmployeeListByTitle(String title) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.title, conditions.Operator.EQUALS, title));
	}
	
	public Dataset<Employee> getEmployeeListByNotes(String notes) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.notes, conditions.Operator.EQUALS, notes));
	}
	
	public Dataset<Employee> getEmployeeListByPhotoPath(String photoPath) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.photoPath, conditions.Operator.EQUALS, photoPath));
	}
	
	public Dataset<Employee> getEmployeeListByTitleOfCourtesy(String titleOfCourtesy) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.titleOfCourtesy, conditions.Operator.EQUALS, titleOfCourtesy));
	}
	
	
	
	public static Dataset<Employee> fullOuterJoinsEmployee(List<Dataset<Employee>> datasetsPOJO) {
		return fullOuterJoinsEmployee(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Employee> fullLeftOuterJoinsEmployee(List<Dataset<Employee>> datasetsPOJO) {
		return fullOuterJoinsEmployee(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Employee> fullOuterJoinsEmployee(List<Dataset<Employee>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Employee> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Employee] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("birthDate", "birthDate_1")
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("country", "country_1")
								.withColumnRenamed("extension", "extension_1")
								.withColumnRenamed("firstname", "firstname_1")
								.withColumnRenamed("hireDate", "hireDate_1")
								.withColumnRenamed("homePhone", "homePhone_1")
								.withColumnRenamed("lastname", "lastname_1")
								.withColumnRenamed("photo", "photo_1")
								.withColumnRenamed("postalCode", "postalCode_1")
								.withColumnRenamed("region", "region_1")
								.withColumnRenamed("salary", "salary_1")
								.withColumnRenamed("title", "title_1")
								.withColumnRenamed("notes", "notes_1")
								.withColumnRenamed("photoPath", "photoPath_1")
								.withColumnRenamed("titleOfCourtesy", "titleOfCourtesy_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("birthDate", "birthDate_" + i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("country", "country_" + i)
								.withColumnRenamed("extension", "extension_" + i)
								.withColumnRenamed("firstname", "firstname_" + i)
								.withColumnRenamed("hireDate", "hireDate_" + i)
								.withColumnRenamed("homePhone", "homePhone_" + i)
								.withColumnRenamed("lastname", "lastname_" + i)
								.withColumnRenamed("photo", "photo_" + i)
								.withColumnRenamed("postalCode", "postalCode_" + i)
								.withColumnRenamed("region", "region_" + i)
								.withColumnRenamed("salary", "salary_" + i)
								.withColumnRenamed("title", "title_" + i)
								.withColumnRenamed("notes", "notes_" + i)
								.withColumnRenamed("photoPath", "photoPath_" + i)
								.withColumnRenamed("titleOfCourtesy", "titleOfCourtesy_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Employee] objects"); 
			d = res.map((MapFunction<Row, Employee>) r -> {
					Employee employee_res = new Employee();
					
					// attribute 'Employee.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					employee_res.setId(firstNotNull_id);
					
					// attribute 'Employee.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.address': " + firstNotNull_address + " and " + address2 + "." );
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					employee_res.setAddress(firstNotNull_address);
					
					// attribute 'Employee.birthDate'
					LocalDate firstNotNull_birthDate = Util.getLocalDateValue(r.getAs("birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate birthDate2 = Util.getLocalDateValue(r.getAs("birthDate_" + i));
						if (firstNotNull_birthDate != null && birthDate2 != null && !firstNotNull_birthDate.equals(birthDate2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_birthDate + " and " + birthDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_birthDate + " and " + birthDate2 + "." );
						}
						if (firstNotNull_birthDate == null && birthDate2 != null) {
							firstNotNull_birthDate = birthDate2;
						}
					}
					employee_res.setBirthDate(firstNotNull_birthDate);
					
					// attribute 'Employee.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.city': " + firstNotNull_city + " and " + city2 + "." );
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					employee_res.setCity(firstNotNull_city);
					
					// attribute 'Employee.country'
					String firstNotNull_country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_country != null && country2 != null && !firstNotNull_country.equals(country2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.country': " + firstNotNull_country + " and " + country2 + "." );
						}
						if (firstNotNull_country == null && country2 != null) {
							firstNotNull_country = country2;
						}
					}
					employee_res.setCountry(firstNotNull_country);
					
					// attribute 'Employee.extension'
					String firstNotNull_extension = Util.getStringValue(r.getAs("extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String extension2 = Util.getStringValue(r.getAs("extension_" + i));
						if (firstNotNull_extension != null && extension2 != null && !firstNotNull_extension.equals(extension2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_extension + " and " + extension2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_extension + " and " + extension2 + "." );
						}
						if (firstNotNull_extension == null && extension2 != null) {
							firstNotNull_extension = extension2;
						}
					}
					employee_res.setExtension(firstNotNull_extension);
					
					// attribute 'Employee.firstname'
					String firstNotNull_firstname = Util.getStringValue(r.getAs("firstname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String firstname2 = Util.getStringValue(r.getAs("firstname_" + i));
						if (firstNotNull_firstname != null && firstname2 != null && !firstNotNull_firstname.equals(firstname2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_firstname + " and " + firstname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.firstname': " + firstNotNull_firstname + " and " + firstname2 + "." );
						}
						if (firstNotNull_firstname == null && firstname2 != null) {
							firstNotNull_firstname = firstname2;
						}
					}
					employee_res.setFirstname(firstNotNull_firstname);
					
					// attribute 'Employee.hireDate'
					LocalDate firstNotNull_hireDate = Util.getLocalDateValue(r.getAs("hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate hireDate2 = Util.getLocalDateValue(r.getAs("hireDate_" + i));
						if (firstNotNull_hireDate != null && hireDate2 != null && !firstNotNull_hireDate.equals(hireDate2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_hireDate + " and " + hireDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_hireDate + " and " + hireDate2 + "." );
						}
						if (firstNotNull_hireDate == null && hireDate2 != null) {
							firstNotNull_hireDate = hireDate2;
						}
					}
					employee_res.setHireDate(firstNotNull_hireDate);
					
					// attribute 'Employee.homePhone'
					String firstNotNull_homePhone = Util.getStringValue(r.getAs("homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String homePhone2 = Util.getStringValue(r.getAs("homePhone_" + i));
						if (firstNotNull_homePhone != null && homePhone2 != null && !firstNotNull_homePhone.equals(homePhone2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_homePhone + " and " + homePhone2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_homePhone + " and " + homePhone2 + "." );
						}
						if (firstNotNull_homePhone == null && homePhone2 != null) {
							firstNotNull_homePhone = homePhone2;
						}
					}
					employee_res.setHomePhone(firstNotNull_homePhone);
					
					// attribute 'Employee.lastname'
					String firstNotNull_lastname = Util.getStringValue(r.getAs("lastname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lastname2 = Util.getStringValue(r.getAs("lastname_" + i));
						if (firstNotNull_lastname != null && lastname2 != null && !firstNotNull_lastname.equals(lastname2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_lastname + " and " + lastname2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.lastname': " + firstNotNull_lastname + " and " + lastname2 + "." );
						}
						if (firstNotNull_lastname == null && lastname2 != null) {
							firstNotNull_lastname = lastname2;
						}
					}
					employee_res.setLastname(firstNotNull_lastname);
					
					// attribute 'Employee.photo'
					String firstNotNull_photo = Util.getStringValue(r.getAs("photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String photo2 = Util.getStringValue(r.getAs("photo_" + i));
						if (firstNotNull_photo != null && photo2 != null && !firstNotNull_photo.equals(photo2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_photo + " and " + photo2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_photo + " and " + photo2 + "." );
						}
						if (firstNotNull_photo == null && photo2 != null) {
							firstNotNull_photo = photo2;
						}
					}
					employee_res.setPhoto(firstNotNull_photo);
					
					// attribute 'Employee.postalCode'
					String firstNotNull_postalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_postalCode != null && postalCode2 != null && !firstNotNull_postalCode.equals(postalCode2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_postalCode == null && postalCode2 != null) {
							firstNotNull_postalCode = postalCode2;
						}
					}
					employee_res.setPostalCode(firstNotNull_postalCode);
					
					// attribute 'Employee.region'
					String firstNotNull_region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_region != null && region2 != null && !firstNotNull_region.equals(region2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.region': " + firstNotNull_region + " and " + region2 + "." );
						}
						if (firstNotNull_region == null && region2 != null) {
							firstNotNull_region = region2;
						}
					}
					employee_res.setRegion(firstNotNull_region);
					
					// attribute 'Employee.salary'
					Double firstNotNull_salary = Util.getDoubleValue(r.getAs("salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double salary2 = Util.getDoubleValue(r.getAs("salary_" + i));
						if (firstNotNull_salary != null && salary2 != null && !firstNotNull_salary.equals(salary2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_salary + " and " + salary2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_salary + " and " + salary2 + "." );
						}
						if (firstNotNull_salary == null && salary2 != null) {
							firstNotNull_salary = salary2;
						}
					}
					employee_res.setSalary(firstNotNull_salary);
					
					// attribute 'Employee.title'
					String firstNotNull_title = Util.getStringValue(r.getAs("title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String title2 = Util.getStringValue(r.getAs("title_" + i));
						if (firstNotNull_title != null && title2 != null && !firstNotNull_title.equals(title2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_title + " and " + title2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.title': " + firstNotNull_title + " and " + title2 + "." );
						}
						if (firstNotNull_title == null && title2 != null) {
							firstNotNull_title = title2;
						}
					}
					employee_res.setTitle(firstNotNull_title);
					
					// attribute 'Employee.notes'
					String firstNotNull_notes = Util.getStringValue(r.getAs("notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String notes2 = Util.getStringValue(r.getAs("notes_" + i));
						if (firstNotNull_notes != null && notes2 != null && !firstNotNull_notes.equals(notes2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_notes + " and " + notes2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_notes + " and " + notes2 + "." );
						}
						if (firstNotNull_notes == null && notes2 != null) {
							firstNotNull_notes = notes2;
						}
					}
					employee_res.setNotes(firstNotNull_notes);
					
					// attribute 'Employee.photoPath'
					String firstNotNull_photoPath = Util.getStringValue(r.getAs("photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String photoPath2 = Util.getStringValue(r.getAs("photoPath_" + i));
						if (firstNotNull_photoPath != null && photoPath2 != null && !firstNotNull_photoPath.equals(photoPath2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_photoPath + " and " + photoPath2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_photoPath + " and " + photoPath2 + "." );
						}
						if (firstNotNull_photoPath == null && photoPath2 != null) {
							firstNotNull_photoPath = photoPath2;
						}
					}
					employee_res.setPhotoPath(firstNotNull_photoPath);
					
					// attribute 'Employee.titleOfCourtesy'
					String firstNotNull_titleOfCourtesy = Util.getStringValue(r.getAs("titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String titleOfCourtesy2 = Util.getStringValue(r.getAs("titleOfCourtesy_" + i));
						if (firstNotNull_titleOfCourtesy != null && titleOfCourtesy2 != null && !firstNotNull_titleOfCourtesy.equals(titleOfCourtesy2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_titleOfCourtesy + " and " + titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getId()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_titleOfCourtesy + " and " + titleOfCourtesy2 + "." );
						}
						if (firstNotNull_titleOfCourtesy == null && titleOfCourtesy2 != null) {
							firstNotNull_titleOfCourtesy = titleOfCourtesy2;
						}
					}
					employee_res.setTitleOfCourtesy(firstNotNull_titleOfCourtesy);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							employee_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							employee_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return employee_res;
				}, Encoders.bean(Employee.class));
			return d;
	}
	
	
	
	
	public Dataset<Employee> getEmployeeList(Employee.are_in role, Territory territory) {
		if(role != null) {
			if(role.equals(Employee.are_in.employee))
				return getEmployeeListInAre_inByTerritory(territory);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.are_in role, Condition<TerritoryAttribute> condition) {
		if(role != null) {
			if(role.equals(Employee.are_in.employee))
				return getEmployeeListInAre_inByTerritoryCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.are_in role, Condition<EmployeeAttribute> condition1, Condition<TerritoryAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employee.are_in.employee))
				return getEmployeeListInAre_in(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Employee> getEmployeeList(Employee.report_to role, Employee employee) {
		if(role != null) {
			if(role.equals(Employee.report_to.lowerEmployee))
				return getLowerEmployeeListInReport_toByHigherEmployee(employee);
		}
		return null;
	}
	
	public Employee getEmployee(Employee.report_to role, Employee employee) {
		if(role != null) {
			if(role.equals(Employee.report_to.higherEmployee))
				return getHigherEmployeeInReport_toByLowerEmployee(employee);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.report_to role, Condition<EmployeeAttribute> condition) {
		if(role != null) {
			if(role.equals(Employee.report_to.lowerEmployee))
				return getLowerEmployeeListInReport_toByHigherEmployeeCondition(condition);
			if(role.equals(Employee.report_to.higherEmployee))
				return getHigherEmployeeListInReport_toByLowerEmployeeCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.report_to role, Condition<EmployeeAttribute> condition1, Condition<EmployeeAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employee.report_to.lowerEmployee))
				return getLowerEmployeeListInReport_to(condition1, condition2);
			if(role.equals(Employee.report_to.higherEmployee))
				return getHigherEmployeeListInReport_to(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Employee getEmployee(Employee.handle role, Order order) {
		if(role != null) {
			if(role.equals(Employee.handle.employee))
				return getEmployeeInHandleByOrder(order);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.handle role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Employee.handle.employee))
				return getEmployeeListInHandleByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.handle role, Condition<EmployeeAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employee.handle.employee))
				return getEmployeeListInHandle(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Employee> getEmployeeListInAre_in(conditions.Condition<conditions.EmployeeAttribute> employee_condition,conditions.Condition<conditions.TerritoryAttribute> territory_condition);
	
	public Dataset<Employee> getEmployeeListInAre_inByEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> employee_condition){
		return getEmployeeListInAre_in(employee_condition, null);
	}
	public Dataset<Employee> getEmployeeListInAre_inByTerritoryCondition(conditions.Condition<conditions.TerritoryAttribute> territory_condition){
		return getEmployeeListInAre_in(null, territory_condition);
	}
	
	public Dataset<Employee> getEmployeeListInAre_inByTerritory(pojo.Territory territory){
		if(territory == null)
			return null;
	
		Condition c;
		c=Condition.simple(TerritoryAttribute.id,Operator.EQUALS, territory.getId());
		Dataset<Employee> res = getEmployeeListInAre_inByTerritoryCondition(c);
		return res;
	}
	
	public abstract Dataset<Employee> getLowerEmployeeListInReport_to(conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition);
	
	public Dataset<Employee> getLowerEmployeeListInReport_toByLowerEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition){
		return getLowerEmployeeListInReport_to(lowerEmployee_condition, null);
	}
	public Dataset<Employee> getLowerEmployeeListInReport_toByHigherEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition){
		return getLowerEmployeeListInReport_to(null, higherEmployee_condition);
	}
	
	public Dataset<Employee> getLowerEmployeeListInReport_toByHigherEmployee(pojo.Employee higherEmployee){
		if(higherEmployee == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeeAttribute.id,Operator.EQUALS, higherEmployee.getId());
		Dataset<Employee> res = getLowerEmployeeListInReport_toByHigherEmployeeCondition(c);
		return res;
	}
	
	public abstract Dataset<Employee> getHigherEmployeeListInReport_to(conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition);
	
	public Dataset<Employee> getHigherEmployeeListInReport_toByLowerEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition){
		return getHigherEmployeeListInReport_to(lowerEmployee_condition, null);
	}
	
	public Employee getHigherEmployeeInReport_toByLowerEmployee(pojo.Employee lowerEmployee){
		if(lowerEmployee == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeeAttribute.id,Operator.EQUALS, lowerEmployee.getId());
		Dataset<Employee> res = getHigherEmployeeListInReport_toByLowerEmployeeCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Employee> getHigherEmployeeListInReport_toByHigherEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition){
		return getHigherEmployeeListInReport_to(null, higherEmployee_condition);
	}
	public abstract Dataset<Employee> getEmployeeListInHandle(conditions.Condition<conditions.EmployeeAttribute> employee_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public Dataset<Employee> getEmployeeListInHandleByEmployeeCondition(conditions.Condition<conditions.EmployeeAttribute> employee_condition){
		return getEmployeeListInHandle(employee_condition, null);
	}
	public Dataset<Employee> getEmployeeListInHandleByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getEmployeeListInHandle(null, order_condition);
	}
	
	public Employee getEmployeeInHandleByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Employee> res = getEmployeeListInHandleByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	public abstract boolean insertEmployee(
		Employee employee,
		 List<Territory> territoryAre_in,
		 List<Order> orderHandle);
	
	public abstract boolean insertEmployeeInEmployeesFromMyMongoDB(Employee employee); 
	private boolean inUpdateMethod = false;
	private List<Row> allEmployeeIdList = null;
	public abstract void updateEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition, conditions.SetClause<conditions.EmployeeAttribute> set);
	
	public void updateEmployee(pojo.Employee employee) {
		//TODO using the id
		return;
	}
	public abstract void updateEmployeeListInAre_in(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		
		conditions.SetClause<conditions.EmployeeAttribute> set
	);
	
	public void updateEmployeeListInAre_inByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateEmployeeListInAre_in(employee_condition, null, set);
	}
	public void updateEmployeeListInAre_inByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateEmployeeListInAre_in(null, territory_condition, set);
	}
	
	public void updateEmployeeListInAre_inByTerritory(
		pojo.Territory territory,
		conditions.SetClause<conditions.EmployeeAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateLowerEmployeeListInReport_to(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition,
		
		conditions.SetClause<conditions.EmployeeAttribute> set
	);
	
	public void updateLowerEmployeeListInReport_toByLowerEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateLowerEmployeeListInReport_to(lowerEmployee_condition, null, set);
	}
	public void updateLowerEmployeeListInReport_toByHigherEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateLowerEmployeeListInReport_to(null, higherEmployee_condition, set);
	}
	
	public void updateLowerEmployeeListInReport_toByHigherEmployee(
		pojo.Employee higherEmployee,
		conditions.SetClause<conditions.EmployeeAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateHigherEmployeeListInReport_to(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition,
		
		conditions.SetClause<conditions.EmployeeAttribute> set
	);
	
	public void updateHigherEmployeeListInReport_toByLowerEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateHigherEmployeeListInReport_to(lowerEmployee_condition, null, set);
	}
	
	public void updateHigherEmployeeInReport_toByLowerEmployee(
		pojo.Employee lowerEmployee,
		conditions.SetClause<conditions.EmployeeAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateHigherEmployeeListInReport_toByHigherEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateHigherEmployeeListInReport_to(null, higherEmployee_condition, set);
	}
	public abstract void updateEmployeeListInHandle(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.EmployeeAttribute> set
	);
	
	public void updateEmployeeListInHandleByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateEmployeeListInHandle(employee_condition, null, set);
	}
	public void updateEmployeeListInHandleByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateEmployeeListInHandle(null, order_condition, set);
	}
	
	public void updateEmployeeInHandleByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.EmployeeAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition);
	
	public void deleteEmployee(pojo.Employee employee) {
		//TODO using the id
		return;
	}
	public abstract void deleteEmployeeListInAre_in(	
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,	
		conditions.Condition<conditions.TerritoryAttribute> territory_condition);
	
	public void deleteEmployeeListInAre_inByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteEmployeeListInAre_in(employee_condition, null);
	}
	public void deleteEmployeeListInAre_inByTerritoryCondition(
		conditions.Condition<conditions.TerritoryAttribute> territory_condition
	){
		deleteEmployeeListInAre_in(null, territory_condition);
	}
	
	public void deleteEmployeeListInAre_inByTerritory(
		pojo.Territory territory 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteLowerEmployeeListInReport_to(	
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,	
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition);
	
	public void deleteLowerEmployeeListInReport_toByLowerEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition
	){
		deleteLowerEmployeeListInReport_to(lowerEmployee_condition, null);
	}
	public void deleteLowerEmployeeListInReport_toByHigherEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition
	){
		deleteLowerEmployeeListInReport_to(null, higherEmployee_condition);
	}
	
	public void deleteLowerEmployeeListInReport_toByHigherEmployee(
		pojo.Employee higherEmployee 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteHigherEmployeeListInReport_to(	
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition,	
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition);
	
	public void deleteHigherEmployeeListInReport_toByLowerEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> lowerEmployee_condition
	){
		deleteHigherEmployeeListInReport_to(lowerEmployee_condition, null);
	}
	
	public void deleteHigherEmployeeInReport_toByLowerEmployee(
		pojo.Employee lowerEmployee 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteHigherEmployeeListInReport_toByHigherEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> higherEmployee_condition
	){
		deleteHigherEmployeeListInReport_to(null, higherEmployee_condition);
	}
	public abstract void deleteEmployeeListInHandle(	
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteEmployeeListInHandleByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteEmployeeListInHandle(employee_condition, null);
	}
	public void deleteEmployeeListInHandleByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteEmployeeListInHandle(null, order_condition);
	}
	
	public void deleteEmployeeInHandleByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
