package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Customer;
import conditions.CustomerAttribute;
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
import conditions.CustomerAttribute;
import pojo.Make_by;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class CustomerService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerService.class);
	protected Make_byService make_byService = new dao.impl.Make_byServiceImpl();
	


	public static enum ROLE_NAME {
		MAKE_BY_CLIENT
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.MAKE_BY_CLIENT, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public CustomerService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public CustomerService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Customer> getCustomerList(){
		return getCustomerList(null);
	}
	
	public Dataset<Customer> getCustomerList(conditions.Condition<conditions.CustomerAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Customer>> datasets = new ArrayList<Dataset<Customer>>();
		Dataset<Customer> d = null;
		d = getCustomerListInOrdersFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getCustomerListInCustomersFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsCustomer(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Customer>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Customer> getCustomerListInOrdersFromMyMongoDB(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	public abstract Dataset<Customer> getCustomerListInCustomersFromMyMongoDB(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Customer getCustomerById(String id){
		Condition cond;
		cond = Condition.simple(CustomerAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Customer> res = getCustomerList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Customer> getCustomerListById(String id) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Customer> getCustomerListByCity(String city) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	public Dataset<Customer> getCustomerListByCompanyName(String companyName) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.companyName, conditions.Operator.EQUALS, companyName));
	}
	
	public Dataset<Customer> getCustomerListByContactName(String contactName) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.contactName, conditions.Operator.EQUALS, contactName));
	}
	
	public Dataset<Customer> getCustomerListByContactTitle(String contactTitle) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.contactTitle, conditions.Operator.EQUALS, contactTitle));
	}
	
	public Dataset<Customer> getCustomerListByCountry(String country) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.country, conditions.Operator.EQUALS, country));
	}
	
	public Dataset<Customer> getCustomerListByFax(String fax) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.fax, conditions.Operator.EQUALS, fax));
	}
	
	public Dataset<Customer> getCustomerListByPhone(String phone) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.phone, conditions.Operator.EQUALS, phone));
	}
	
	public Dataset<Customer> getCustomerListByPostalCode(String postalCode) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.postalCode, conditions.Operator.EQUALS, postalCode));
	}
	
	public Dataset<Customer> getCustomerListByRegion(String region) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.region, conditions.Operator.EQUALS, region));
	}
	
	public Dataset<Customer> getCustomerListByAddress(String address) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	
	
	public static Dataset<Customer> fullOuterJoinsCustomer(List<Dataset<Customer>> datasetsPOJO) {
		return fullOuterJoinsCustomer(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Customer> fullLeftOuterJoinsCustomer(List<Dataset<Customer>> datasetsPOJO) {
		return fullOuterJoinsCustomer(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Customer> fullOuterJoinsCustomer(List<Dataset<Customer>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Customer> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Customer] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("companyName", "companyName_1")
								.withColumnRenamed("contactName", "contactName_1")
								.withColumnRenamed("contactTitle", "contactTitle_1")
								.withColumnRenamed("country", "country_1")
								.withColumnRenamed("fax", "fax_1")
								.withColumnRenamed("phone", "phone_1")
								.withColumnRenamed("postalCode", "postalCode_1")
								.withColumnRenamed("region", "region_1")
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("companyName", "companyName_" + i)
								.withColumnRenamed("contactName", "contactName_" + i)
								.withColumnRenamed("contactTitle", "contactTitle_" + i)
								.withColumnRenamed("country", "country_" + i)
								.withColumnRenamed("fax", "fax_" + i)
								.withColumnRenamed("phone", "phone_" + i)
								.withColumnRenamed("postalCode", "postalCode_" + i)
								.withColumnRenamed("region", "region_" + i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Customer] objects"); 
			d = res.map((MapFunction<Row, Customer>) r -> {
					Customer customer_res = new Customer();
					
					// attribute 'Customer.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					customer_res.setId(firstNotNull_id);
					
					// attribute 'Customer.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.city': " + firstNotNull_city + " and " + city2 + "." );
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					customer_res.setCity(firstNotNull_city);
					
					// attribute 'Customer.companyName'
					String firstNotNull_companyName = Util.getStringValue(r.getAs("companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String companyName2 = Util.getStringValue(r.getAs("companyName_" + i));
						if (firstNotNull_companyName != null && companyName2 != null && !firstNotNull_companyName.equals(companyName2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
						}
						if (firstNotNull_companyName == null && companyName2 != null) {
							firstNotNull_companyName = companyName2;
						}
					}
					customer_res.setCompanyName(firstNotNull_companyName);
					
					// attribute 'Customer.contactName'
					String firstNotNull_contactName = Util.getStringValue(r.getAs("contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactName2 = Util.getStringValue(r.getAs("contactName_" + i));
						if (firstNotNull_contactName != null && contactName2 != null && !firstNotNull_contactName.equals(contactName2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.contactName': " + firstNotNull_contactName + " and " + contactName2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.contactName': " + firstNotNull_contactName + " and " + contactName2 + "." );
						}
						if (firstNotNull_contactName == null && contactName2 != null) {
							firstNotNull_contactName = contactName2;
						}
					}
					customer_res.setContactName(firstNotNull_contactName);
					
					// attribute 'Customer.contactTitle'
					String firstNotNull_contactTitle = Util.getStringValue(r.getAs("contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactTitle2 = Util.getStringValue(r.getAs("contactTitle_" + i));
						if (firstNotNull_contactTitle != null && contactTitle2 != null && !firstNotNull_contactTitle.equals(contactTitle2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.contactTitle': " + firstNotNull_contactTitle + " and " + contactTitle2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.contactTitle': " + firstNotNull_contactTitle + " and " + contactTitle2 + "." );
						}
						if (firstNotNull_contactTitle == null && contactTitle2 != null) {
							firstNotNull_contactTitle = contactTitle2;
						}
					}
					customer_res.setContactTitle(firstNotNull_contactTitle);
					
					// attribute 'Customer.country'
					String firstNotNull_country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_country != null && country2 != null && !firstNotNull_country.equals(country2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.country': " + firstNotNull_country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.country': " + firstNotNull_country + " and " + country2 + "." );
						}
						if (firstNotNull_country == null && country2 != null) {
							firstNotNull_country = country2;
						}
					}
					customer_res.setCountry(firstNotNull_country);
					
					// attribute 'Customer.fax'
					String firstNotNull_fax = Util.getStringValue(r.getAs("fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String fax2 = Util.getStringValue(r.getAs("fax_" + i));
						if (firstNotNull_fax != null && fax2 != null && !firstNotNull_fax.equals(fax2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.fax': " + firstNotNull_fax + " and " + fax2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.fax': " + firstNotNull_fax + " and " + fax2 + "." );
						}
						if (firstNotNull_fax == null && fax2 != null) {
							firstNotNull_fax = fax2;
						}
					}
					customer_res.setFax(firstNotNull_fax);
					
					// attribute 'Customer.phone'
					String firstNotNull_phone = Util.getStringValue(r.getAs("phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String phone2 = Util.getStringValue(r.getAs("phone_" + i));
						if (firstNotNull_phone != null && phone2 != null && !firstNotNull_phone.equals(phone2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.phone': " + firstNotNull_phone + " and " + phone2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.phone': " + firstNotNull_phone + " and " + phone2 + "." );
						}
						if (firstNotNull_phone == null && phone2 != null) {
							firstNotNull_phone = phone2;
						}
					}
					customer_res.setPhone(firstNotNull_phone);
					
					// attribute 'Customer.postalCode'
					String firstNotNull_postalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_postalCode != null && postalCode2 != null && !firstNotNull_postalCode.equals(postalCode2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_postalCode == null && postalCode2 != null) {
							firstNotNull_postalCode = postalCode2;
						}
					}
					customer_res.setPostalCode(firstNotNull_postalCode);
					
					// attribute 'Customer.region'
					String firstNotNull_region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_region != null && region2 != null && !firstNotNull_region.equals(region2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.region': " + firstNotNull_region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.region': " + firstNotNull_region + " and " + region2 + "." );
						}
						if (firstNotNull_region == null && region2 != null) {
							firstNotNull_region = region2;
						}
					}
					customer_res.setRegion(firstNotNull_region);
					
					// attribute 'Customer.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.address': " + firstNotNull_address + " and " + address2 + "." );
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					customer_res.setAddress(firstNotNull_address);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							customer_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							customer_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return customer_res;
				}, Encoders.bean(Customer.class));
			return d;
	}
	
	
	public Dataset<Customer> getCustomerList(Customer.make_by role, Order order) {
		if(role != null) {
			if(role.equals(Customer.make_by.client))
				return getClientListInMake_byByOrder(order);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.make_by role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Customer.make_by.client))
				return getClientListInMake_byByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.make_by role, Condition<OrderAttribute> condition1, Condition<CustomerAttribute> condition2) {
		if(role != null) {
			if(role.equals(Customer.make_by.client))
				return getClientListInMake_by(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Customer> getClientListInMake_by(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public Dataset<Customer> getClientListInMake_byByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getClientListInMake_by(order_condition, null);
	}
	
	public Dataset<Customer> getClientListInMake_byByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Customer> res = getClientListInMake_byByOrderCondition(c);
		return res;
	}
	
	public Dataset<Customer> getClientListInMake_byByClientCondition(conditions.Condition<conditions.CustomerAttribute> client_condition){
		return getClientListInMake_by(null, client_condition);
	}
	
	public abstract boolean insertCustomer(
		Customer customer,
		Order	orderMake_by);
	
	public abstract boolean insertCustomerInCustomersFromMyMongoDB(Customer customer); 
	private boolean inUpdateMethod = false;
	private List<Row> allCustomerIdList = null;
	public abstract void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set);
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public abstract void updateClientListInMake_by(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	);
	
	public void updateClientListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInMake_by(order_condition, null, set);
	}
	
	public void updateClientListInMake_byByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateClientListInMake_byByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInMake_by(null, client_condition, set);
	}
	
	
	public abstract void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition);
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public abstract void deleteClientListInMake_by(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public void deleteClientListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteClientListInMake_by(order_condition, null);
	}
	
	public void deleteClientListInMake_byByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteClientListInMake_byByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteClientListInMake_by(null, client_condition);
	}
	
}
