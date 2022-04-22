package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Supplier;
import conditions.SupplierAttribute;
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
import conditions.SupplierAttribute;
import pojo.Insert;
import conditions.ProductAttribute;
import pojo.Product;

public abstract class SupplierService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SupplierService.class);
	protected InsertService insertService = new dao.impl.InsertServiceImpl();
	


	public static enum ROLE_NAME {
		INSERT_SUPPLIER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.INSERT_SUPPLIER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public SupplierService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public SupplierService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Supplier> getSupplierList(){
		return getSupplierList(null);
	}
	
	public Dataset<Supplier> getSupplierList(conditions.Condition<conditions.SupplierAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Supplier>> datasets = new ArrayList<Dataset<Supplier>>();
		Dataset<Supplier> d = null;
		d = getSupplierListInSuppliersFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsSupplier(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Supplier>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Supplier> getSupplierListInSuppliersFromMyMongoDB(conditions.Condition<conditions.SupplierAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Supplier getSupplierById(Integer id){
		Condition cond;
		cond = Condition.simple(SupplierAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Supplier> res = getSupplierList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Supplier> getSupplierListById(Integer id) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Supplier> getSupplierListByAddress(String address) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	public Dataset<Supplier> getSupplierListByCity(String city) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	public Dataset<Supplier> getSupplierListByCompanyName(String companyName) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.companyName, conditions.Operator.EQUALS, companyName));
	}
	
	public Dataset<Supplier> getSupplierListByContactName(String contactName) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.contactName, conditions.Operator.EQUALS, contactName));
	}
	
	public Dataset<Supplier> getSupplierListByContactTitle(String contactTitle) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.contactTitle, conditions.Operator.EQUALS, contactTitle));
	}
	
	public Dataset<Supplier> getSupplierListByCountry(String country) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.country, conditions.Operator.EQUALS, country));
	}
	
	public Dataset<Supplier> getSupplierListByFax(String fax) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.fax, conditions.Operator.EQUALS, fax));
	}
	
	public Dataset<Supplier> getSupplierListByHomePage(String homePage) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.homePage, conditions.Operator.EQUALS, homePage));
	}
	
	public Dataset<Supplier> getSupplierListByPhone(String phone) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.phone, conditions.Operator.EQUALS, phone));
	}
	
	public Dataset<Supplier> getSupplierListByPostalCode(String postalCode) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.postalCode, conditions.Operator.EQUALS, postalCode));
	}
	
	public Dataset<Supplier> getSupplierListByRegion(String region) {
		return getSupplierList(conditions.Condition.simple(conditions.SupplierAttribute.region, conditions.Operator.EQUALS, region));
	}
	
	
	
	public static Dataset<Supplier> fullOuterJoinsSupplier(List<Dataset<Supplier>> datasetsPOJO) {
		return fullOuterJoinsSupplier(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Supplier> fullLeftOuterJoinsSupplier(List<Dataset<Supplier>> datasetsPOJO) {
		return fullOuterJoinsSupplier(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Supplier> fullOuterJoinsSupplier(List<Dataset<Supplier>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Supplier> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Supplier] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("companyName", "companyName_1")
								.withColumnRenamed("contactName", "contactName_1")
								.withColumnRenamed("contactTitle", "contactTitle_1")
								.withColumnRenamed("country", "country_1")
								.withColumnRenamed("fax", "fax_1")
								.withColumnRenamed("homePage", "homePage_1")
								.withColumnRenamed("phone", "phone_1")
								.withColumnRenamed("postalCode", "postalCode_1")
								.withColumnRenamed("region", "region_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("companyName", "companyName_" + i)
								.withColumnRenamed("contactName", "contactName_" + i)
								.withColumnRenamed("contactTitle", "contactTitle_" + i)
								.withColumnRenamed("country", "country_" + i)
								.withColumnRenamed("fax", "fax_" + i)
								.withColumnRenamed("homePage", "homePage_" + i)
								.withColumnRenamed("phone", "phone_" + i)
								.withColumnRenamed("postalCode", "postalCode_" + i)
								.withColumnRenamed("region", "region_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Supplier] objects"); 
			d = res.map((MapFunction<Row, Supplier>) r -> {
					Supplier supplier_res = new Supplier();
					
					// attribute 'Supplier.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					supplier_res.setId(firstNotNull_id);
					
					// attribute 'Supplier.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.address': " + firstNotNull_address + " and " + address2 + "." );
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					supplier_res.setAddress(firstNotNull_address);
					
					// attribute 'Supplier.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.city': " + firstNotNull_city + " and " + city2 + "." );
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					supplier_res.setCity(firstNotNull_city);
					
					// attribute 'Supplier.companyName'
					String firstNotNull_companyName = Util.getStringValue(r.getAs("companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String companyName2 = Util.getStringValue(r.getAs("companyName_" + i));
						if (firstNotNull_companyName != null && companyName2 != null && !firstNotNull_companyName.equals(companyName2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
						}
						if (firstNotNull_companyName == null && companyName2 != null) {
							firstNotNull_companyName = companyName2;
						}
					}
					supplier_res.setCompanyName(firstNotNull_companyName);
					
					// attribute 'Supplier.contactName'
					String firstNotNull_contactName = Util.getStringValue(r.getAs("contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactName2 = Util.getStringValue(r.getAs("contactName_" + i));
						if (firstNotNull_contactName != null && contactName2 != null && !firstNotNull_contactName.equals(contactName2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactName': " + firstNotNull_contactName + " and " + contactName2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactName': " + firstNotNull_contactName + " and " + contactName2 + "." );
						}
						if (firstNotNull_contactName == null && contactName2 != null) {
							firstNotNull_contactName = contactName2;
						}
					}
					supplier_res.setContactName(firstNotNull_contactName);
					
					// attribute 'Supplier.contactTitle'
					String firstNotNull_contactTitle = Util.getStringValue(r.getAs("contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactTitle2 = Util.getStringValue(r.getAs("contactTitle_" + i));
						if (firstNotNull_contactTitle != null && contactTitle2 != null && !firstNotNull_contactTitle.equals(contactTitle2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactTitle': " + firstNotNull_contactTitle + " and " + contactTitle2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactTitle': " + firstNotNull_contactTitle + " and " + contactTitle2 + "." );
						}
						if (firstNotNull_contactTitle == null && contactTitle2 != null) {
							firstNotNull_contactTitle = contactTitle2;
						}
					}
					supplier_res.setContactTitle(firstNotNull_contactTitle);
					
					// attribute 'Supplier.country'
					String firstNotNull_country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_country != null && country2 != null && !firstNotNull_country.equals(country2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.country': " + firstNotNull_country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.country': " + firstNotNull_country + " and " + country2 + "." );
						}
						if (firstNotNull_country == null && country2 != null) {
							firstNotNull_country = country2;
						}
					}
					supplier_res.setCountry(firstNotNull_country);
					
					// attribute 'Supplier.fax'
					String firstNotNull_fax = Util.getStringValue(r.getAs("fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String fax2 = Util.getStringValue(r.getAs("fax_" + i));
						if (firstNotNull_fax != null && fax2 != null && !firstNotNull_fax.equals(fax2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.fax': " + firstNotNull_fax + " and " + fax2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.fax': " + firstNotNull_fax + " and " + fax2 + "." );
						}
						if (firstNotNull_fax == null && fax2 != null) {
							firstNotNull_fax = fax2;
						}
					}
					supplier_res.setFax(firstNotNull_fax);
					
					// attribute 'Supplier.homePage'
					String firstNotNull_homePage = Util.getStringValue(r.getAs("homePage"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String homePage2 = Util.getStringValue(r.getAs("homePage_" + i));
						if (firstNotNull_homePage != null && homePage2 != null && !firstNotNull_homePage.equals(homePage2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.homePage': " + firstNotNull_homePage + " and " + homePage2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.homePage': " + firstNotNull_homePage + " and " + homePage2 + "." );
						}
						if (firstNotNull_homePage == null && homePage2 != null) {
							firstNotNull_homePage = homePage2;
						}
					}
					supplier_res.setHomePage(firstNotNull_homePage);
					
					// attribute 'Supplier.phone'
					String firstNotNull_phone = Util.getStringValue(r.getAs("phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String phone2 = Util.getStringValue(r.getAs("phone_" + i));
						if (firstNotNull_phone != null && phone2 != null && !firstNotNull_phone.equals(phone2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.phone': " + firstNotNull_phone + " and " + phone2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.phone': " + firstNotNull_phone + " and " + phone2 + "." );
						}
						if (firstNotNull_phone == null && phone2 != null) {
							firstNotNull_phone = phone2;
						}
					}
					supplier_res.setPhone(firstNotNull_phone);
					
					// attribute 'Supplier.postalCode'
					String firstNotNull_postalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_postalCode != null && postalCode2 != null && !firstNotNull_postalCode.equals(postalCode2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_postalCode == null && postalCode2 != null) {
							firstNotNull_postalCode = postalCode2;
						}
					}
					supplier_res.setPostalCode(firstNotNull_postalCode);
					
					// attribute 'Supplier.region'
					String firstNotNull_region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_region != null && region2 != null && !firstNotNull_region.equals(region2)) {
							supplier_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.region': " + firstNotNull_region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.region': " + firstNotNull_region + " and " + region2 + "." );
						}
						if (firstNotNull_region == null && region2 != null) {
							firstNotNull_region = region2;
						}
					}
					supplier_res.setRegion(firstNotNull_region);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							supplier_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							supplier_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return supplier_res;
				}, Encoders.bean(Supplier.class));
			return d;
	}
	
	
	
	
	
	
	
	
	public Supplier getSupplier(Supplier.insert role, Product product) {
		if(role != null) {
			if(role.equals(Supplier.insert.supplier))
				return getSupplierInInsertByProduct(product);
		}
		return null;
	}
	
	public Dataset<Supplier> getSupplierList(Supplier.insert role, Condition<ProductAttribute> condition) {
		if(role != null) {
			if(role.equals(Supplier.insert.supplier))
				return getSupplierListInInsertByProductCondition(condition);
		}
		return null;
	}
	
	public Dataset<Supplier> getSupplierList(Supplier.insert role, Condition<SupplierAttribute> condition1, Condition<ProductAttribute> condition2) {
		if(role != null) {
			if(role.equals(Supplier.insert.supplier))
				return getSupplierListInInsert(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Supplier> getSupplierListInInsert(conditions.Condition<conditions.SupplierAttribute> supplier_condition,conditions.Condition<conditions.ProductAttribute> product_condition);
	
	public Dataset<Supplier> getSupplierListInInsertBySupplierCondition(conditions.Condition<conditions.SupplierAttribute> supplier_condition){
		return getSupplierListInInsert(supplier_condition, null);
	}
	public Dataset<Supplier> getSupplierListInInsertByProductCondition(conditions.Condition<conditions.ProductAttribute> product_condition){
		return getSupplierListInInsert(null, product_condition);
	}
	
	public Supplier getSupplierInInsertByProduct(pojo.Product product){
		if(product == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, product.getId());
		Dataset<Supplier> res = getSupplierListInInsertByProductCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	public abstract boolean insertSupplier(
		Supplier supplier,
		 List<Product> productInsert);
	
	public abstract boolean insertSupplierInSuppliersFromMyMongoDB(Supplier supplier); 
	private boolean inUpdateMethod = false;
	private List<Row> allSupplierIdList = null;
	public abstract void updateSupplierList(conditions.Condition<conditions.SupplierAttribute> condition, conditions.SetClause<conditions.SupplierAttribute> set);
	
	public void updateSupplier(pojo.Supplier supplier) {
		//TODO using the id
		return;
	}
	public abstract void updateSupplierListInInsert(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		
		conditions.SetClause<conditions.SupplierAttribute> set
	);
	
	public void updateSupplierListInInsertBySupplierCondition(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.SetClause<conditions.SupplierAttribute> set
	){
		updateSupplierListInInsert(supplier_condition, null, set);
	}
	public void updateSupplierListInInsertByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.SetClause<conditions.SupplierAttribute> set
	){
		updateSupplierListInInsert(null, product_condition, set);
	}
	
	public void updateSupplierInInsertByProduct(
		pojo.Product product,
		conditions.SetClause<conditions.SupplierAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteSupplierList(conditions.Condition<conditions.SupplierAttribute> condition);
	
	public void deleteSupplier(pojo.Supplier supplier) {
		//TODO using the id
		return;
	}
	public abstract void deleteSupplierListInInsert(	
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,	
		conditions.Condition<conditions.ProductAttribute> product_condition);
	
	public void deleteSupplierListInInsertBySupplierCondition(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition
	){
		deleteSupplierListInInsert(supplier_condition, null);
	}
	public void deleteSupplierListInInsertByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteSupplierListInInsert(null, product_condition);
	}
	
	public void deleteSupplierInInsertByProduct(
		pojo.Product product 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
