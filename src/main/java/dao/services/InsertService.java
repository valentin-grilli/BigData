package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Insert;
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


public abstract class InsertService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertService.class);
	
	
	// Left side 'SupplierRef' of reference [supplierR ]
	public abstract Dataset<ProductTDO> getProductTDOListProductInSupplierRInProductsInfoFromRelSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'SupplierID' of reference [supplierR ]
	public abstract Dataset<SupplierTDO> getSupplierTDOListSupplierInSupplierRInProductsInfoFromRelSchema(Condition<SupplierAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Insert> fullLeftOuterJoinBetweenInsertAndSupplier(Dataset<Insert> d1, Dataset<Supplier> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("contactName", "A_contactName")
			.withColumnRenamed("contactTitle", "A_contactTitle")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("fax", "A_fax")
			.withColumnRenamed("homePage", "A_homePage")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("supplier.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Insert>) r -> {
				Insert res = new Insert();
	
				Supplier supplier = new Supplier();
				Object o = r.getAs("supplier");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						supplier.setId(Util.getIntegerValue(r2.getAs("id")));
						supplier.setAddress(Util.getStringValue(r2.getAs("address")));
						supplier.setCity(Util.getStringValue(r2.getAs("city")));
						supplier.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						supplier.setContactName(Util.getStringValue(r2.getAs("contactName")));
						supplier.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						supplier.setCountry(Util.getStringValue(r2.getAs("country")));
						supplier.setFax(Util.getStringValue(r2.getAs("fax")));
						supplier.setHomePage(Util.getStringValue(r2.getAs("homePage")));
						supplier.setPhone(Util.getStringValue(r2.getAs("phone")));
						supplier.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						supplier.setRegion(Util.getStringValue(r2.getAs("region")));
					} 
					if(o instanceof Supplier) {
						supplier = (Supplier) o;
					}
				}
	
				res.setSupplier(supplier);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (supplier.getId() != null && id != null && !supplier.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.id': " + supplier.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.id': " + supplier.getId() + " and " + id + "." );
				}
				if(id != null)
					supplier.setId(id);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (supplier.getAddress() != null && address != null && !supplier.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.address': " + supplier.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.address': " + supplier.getAddress() + " and " + address + "." );
				}
				if(address != null)
					supplier.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (supplier.getCity() != null && city != null && !supplier.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.city': " + supplier.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.city': " + supplier.getCity() + " and " + city + "." );
				}
				if(city != null)
					supplier.setCity(city);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (supplier.getCompanyName() != null && companyName != null && !supplier.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.companyName': " + supplier.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.companyName': " + supplier.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					supplier.setCompanyName(companyName);
				String contactName = Util.getStringValue(r.getAs("A_contactName"));
				if (supplier.getContactName() != null && contactName != null && !supplier.getContactName().equals(contactName)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.contactName': " + supplier.getContactName() + " and " + contactName + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.contactName': " + supplier.getContactName() + " and " + contactName + "." );
				}
				if(contactName != null)
					supplier.setContactName(contactName);
				String contactTitle = Util.getStringValue(r.getAs("A_contactTitle"));
				if (supplier.getContactTitle() != null && contactTitle != null && !supplier.getContactTitle().equals(contactTitle)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.contactTitle': " + supplier.getContactTitle() + " and " + contactTitle + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.contactTitle': " + supplier.getContactTitle() + " and " + contactTitle + "." );
				}
				if(contactTitle != null)
					supplier.setContactTitle(contactTitle);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (supplier.getCountry() != null && country != null && !supplier.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.country': " + supplier.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.country': " + supplier.getCountry() + " and " + country + "." );
				}
				if(country != null)
					supplier.setCountry(country);
				String fax = Util.getStringValue(r.getAs("A_fax"));
				if (supplier.getFax() != null && fax != null && !supplier.getFax().equals(fax)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.fax': " + supplier.getFax() + " and " + fax + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.fax': " + supplier.getFax() + " and " + fax + "." );
				}
				if(fax != null)
					supplier.setFax(fax);
				String homePage = Util.getStringValue(r.getAs("A_homePage"));
				if (supplier.getHomePage() != null && homePage != null && !supplier.getHomePage().equals(homePage)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.homePage': " + supplier.getHomePage() + " and " + homePage + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.homePage': " + supplier.getHomePage() + " and " + homePage + "." );
				}
				if(homePage != null)
					supplier.setHomePage(homePage);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (supplier.getPhone() != null && phone != null && !supplier.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.phone': " + supplier.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.phone': " + supplier.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					supplier.setPhone(phone);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (supplier.getPostalCode() != null && postalCode != null && !supplier.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.postalCode': " + supplier.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.postalCode': " + supplier.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					supplier.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (supplier.getRegion() != null && region != null && !supplier.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.region': " + supplier.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.region': " + supplier.getRegion() + " and " + region + "." );
				}
				if(region != null)
					supplier.setRegion(region);
	
				o = r.getAs("product");
				Product product = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setId(Util.getIntegerValue(r2.getAs("id")));
						product.setName(Util.getStringValue(r2.getAs("name")));
						product.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
						product.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof Product) {
						product = (Product) o;
					}
				}
	
				res.setProduct(product);
	
				return res;
		}, Encoders.bean(Insert.class));
	
		
		
	}
	public static Dataset<Insert> fullLeftOuterJoinBetweenInsertAndProduct(Dataset<Insert> d1, Dataset<Product> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("name", "A_name")
			.withColumnRenamed("supplierRef", "A_supplierRef")
			.withColumnRenamed("categoryRef", "A_categoryRef")
			.withColumnRenamed("quantityPerUnit", "A_quantityPerUnit")
			.withColumnRenamed("unitPrice", "A_unitPrice")
			.withColumnRenamed("reorderLevel", "A_reorderLevel")
			.withColumnRenamed("discontinued", "A_discontinued")
			.withColumnRenamed("unitsInStock", "A_unitsInStock")
			.withColumnRenamed("unitsOnOrder", "A_unitsOnOrder")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("product.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Insert>) r -> {
				Insert res = new Insert();
	
				Product product = new Product();
				Object o = r.getAs("product");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setId(Util.getIntegerValue(r2.getAs("id")));
						product.setName(Util.getStringValue(r2.getAs("name")));
						product.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
						product.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof Product) {
						product = (Product) o;
					}
				}
	
				res.setProduct(product);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (product.getId() != null && id != null && !product.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.id': " + product.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.id': " + product.getId() + " and " + id + "." );
				}
				if(id != null)
					product.setId(id);
				String name = Util.getStringValue(r.getAs("A_name"));
				if (product.getName() != null && name != null && !product.getName().equals(name)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.name': " + product.getName() + " and " + name + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.name': " + product.getName() + " and " + name + "." );
				}
				if(name != null)
					product.setName(name);
				Integer supplierRef = Util.getIntegerValue(r.getAs("A_supplierRef"));
				if (product.getSupplierRef() != null && supplierRef != null && !product.getSupplierRef().equals(supplierRef)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
				}
				if(supplierRef != null)
					product.setSupplierRef(supplierRef);
				Integer categoryRef = Util.getIntegerValue(r.getAs("A_categoryRef"));
				if (product.getCategoryRef() != null && categoryRef != null && !product.getCategoryRef().equals(categoryRef)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
				}
				if(categoryRef != null)
					product.setCategoryRef(categoryRef);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (product.getQuantityPerUnit() != null && quantityPerUnit != null && !product.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					product.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (product.getUnitPrice() != null && unitPrice != null && !product.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					product.setUnitPrice(unitPrice);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (product.getReorderLevel() != null && reorderLevel != null && !product.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					product.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (product.getDiscontinued() != null && discontinued != null && !product.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					product.setDiscontinued(discontinued);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (product.getUnitsInStock() != null && unitsInStock != null && !product.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					product.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (product.getUnitsOnOrder() != null && unitsOnOrder != null && !product.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Insert - different values found for attribute 'Insert.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Insert - different values found for attribute 'Insert.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					product.setUnitsOnOrder(unitsOnOrder);
	
				o = r.getAs("supplier");
				Supplier supplier = new Supplier();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						supplier.setId(Util.getIntegerValue(r2.getAs("id")));
						supplier.setAddress(Util.getStringValue(r2.getAs("address")));
						supplier.setCity(Util.getStringValue(r2.getAs("city")));
						supplier.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						supplier.setContactName(Util.getStringValue(r2.getAs("contactName")));
						supplier.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						supplier.setCountry(Util.getStringValue(r2.getAs("country")));
						supplier.setFax(Util.getStringValue(r2.getAs("fax")));
						supplier.setHomePage(Util.getStringValue(r2.getAs("homePage")));
						supplier.setPhone(Util.getStringValue(r2.getAs("phone")));
						supplier.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						supplier.setRegion(Util.getStringValue(r2.getAs("region")));
					} 
					if(o instanceof Supplier) {
						supplier = (Supplier) o;
					}
				}
	
				res.setSupplier(supplier);
	
				return res;
		}, Encoders.bean(Insert.class));
	
		
		
	}
	
	public static Dataset<Insert> fullOuterJoinsInsert(List<Dataset<Insert>> datasetsPOJO) {
		return fullOuterJoinsInsert(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Insert> fullLeftOuterJoinsInsert(List<Dataset<Insert>> datasetsPOJO) {
		return fullOuterJoinsInsert(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Insert> fullOuterJoinsInsert(List<Dataset<Insert>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("supplier.id");
	
		idFields.add("product.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Insert> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("supplier_id_" + i, d.col("supplier.id"))
				.withColumn("product_id_" + i, d.col("product.id"))
				.withColumnRenamed("supplier", "supplier_" + i)
				.withColumnRenamed("product", "product_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("supplier_id_0").equalTo(rows.get(1).col("supplier_id_1"));
		joinCond = joinCond.and(rows.get(0).col("product_id_0").equalTo(rows.get(1).col("product_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("supplier_id_" + (i - 1)).equalTo(rows.get(i).col("supplier_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("product_id_" + (i - 1)).equalTo(rows.get(i).col("product_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Insert>) r -> {
				Insert insert_res = new Insert();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							insert_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							insert_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Supplier supplier_res = new Supplier();
					Product product_res = new Product();
					
					// attribute 'Supplier.id'
					Integer firstNotNull_supplier_id = Util.getIntegerValue(r.getAs("supplier_0.id"));
					supplier_res.setId(firstNotNull_supplier_id);
					// attribute 'Supplier.address'
					String firstNotNull_supplier_address = Util.getStringValue(r.getAs("supplier_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_address2 = Util.getStringValue(r.getAs("supplier_" + i + ".address"));
						if (firstNotNull_supplier_address != null && supplier_address2 != null && !firstNotNull_supplier_address.equals(supplier_address2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.address': " + firstNotNull_supplier_address + " and " + supplier_address2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.address': " + firstNotNull_supplier_address + " and " + supplier_address2 + "." );
						}
						if (firstNotNull_supplier_address == null && supplier_address2 != null) {
							firstNotNull_supplier_address = supplier_address2;
						}
					}
					supplier_res.setAddress(firstNotNull_supplier_address);
					// attribute 'Supplier.city'
					String firstNotNull_supplier_city = Util.getStringValue(r.getAs("supplier_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_city2 = Util.getStringValue(r.getAs("supplier_" + i + ".city"));
						if (firstNotNull_supplier_city != null && supplier_city2 != null && !firstNotNull_supplier_city.equals(supplier_city2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.city': " + firstNotNull_supplier_city + " and " + supplier_city2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.city': " + firstNotNull_supplier_city + " and " + supplier_city2 + "." );
						}
						if (firstNotNull_supplier_city == null && supplier_city2 != null) {
							firstNotNull_supplier_city = supplier_city2;
						}
					}
					supplier_res.setCity(firstNotNull_supplier_city);
					// attribute 'Supplier.companyName'
					String firstNotNull_supplier_companyName = Util.getStringValue(r.getAs("supplier_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_companyName2 = Util.getStringValue(r.getAs("supplier_" + i + ".companyName"));
						if (firstNotNull_supplier_companyName != null && supplier_companyName2 != null && !firstNotNull_supplier_companyName.equals(supplier_companyName2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.companyName': " + firstNotNull_supplier_companyName + " and " + supplier_companyName2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.companyName': " + firstNotNull_supplier_companyName + " and " + supplier_companyName2 + "." );
						}
						if (firstNotNull_supplier_companyName == null && supplier_companyName2 != null) {
							firstNotNull_supplier_companyName = supplier_companyName2;
						}
					}
					supplier_res.setCompanyName(firstNotNull_supplier_companyName);
					// attribute 'Supplier.contactName'
					String firstNotNull_supplier_contactName = Util.getStringValue(r.getAs("supplier_0.contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_contactName2 = Util.getStringValue(r.getAs("supplier_" + i + ".contactName"));
						if (firstNotNull_supplier_contactName != null && supplier_contactName2 != null && !firstNotNull_supplier_contactName.equals(supplier_contactName2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactName': " + firstNotNull_supplier_contactName + " and " + supplier_contactName2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactName': " + firstNotNull_supplier_contactName + " and " + supplier_contactName2 + "." );
						}
						if (firstNotNull_supplier_contactName == null && supplier_contactName2 != null) {
							firstNotNull_supplier_contactName = supplier_contactName2;
						}
					}
					supplier_res.setContactName(firstNotNull_supplier_contactName);
					// attribute 'Supplier.contactTitle'
					String firstNotNull_supplier_contactTitle = Util.getStringValue(r.getAs("supplier_0.contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_contactTitle2 = Util.getStringValue(r.getAs("supplier_" + i + ".contactTitle"));
						if (firstNotNull_supplier_contactTitle != null && supplier_contactTitle2 != null && !firstNotNull_supplier_contactTitle.equals(supplier_contactTitle2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactTitle': " + firstNotNull_supplier_contactTitle + " and " + supplier_contactTitle2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.contactTitle': " + firstNotNull_supplier_contactTitle + " and " + supplier_contactTitle2 + "." );
						}
						if (firstNotNull_supplier_contactTitle == null && supplier_contactTitle2 != null) {
							firstNotNull_supplier_contactTitle = supplier_contactTitle2;
						}
					}
					supplier_res.setContactTitle(firstNotNull_supplier_contactTitle);
					// attribute 'Supplier.country'
					String firstNotNull_supplier_country = Util.getStringValue(r.getAs("supplier_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_country2 = Util.getStringValue(r.getAs("supplier_" + i + ".country"));
						if (firstNotNull_supplier_country != null && supplier_country2 != null && !firstNotNull_supplier_country.equals(supplier_country2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.country': " + firstNotNull_supplier_country + " and " + supplier_country2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.country': " + firstNotNull_supplier_country + " and " + supplier_country2 + "." );
						}
						if (firstNotNull_supplier_country == null && supplier_country2 != null) {
							firstNotNull_supplier_country = supplier_country2;
						}
					}
					supplier_res.setCountry(firstNotNull_supplier_country);
					// attribute 'Supplier.fax'
					String firstNotNull_supplier_fax = Util.getStringValue(r.getAs("supplier_0.fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_fax2 = Util.getStringValue(r.getAs("supplier_" + i + ".fax"));
						if (firstNotNull_supplier_fax != null && supplier_fax2 != null && !firstNotNull_supplier_fax.equals(supplier_fax2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.fax': " + firstNotNull_supplier_fax + " and " + supplier_fax2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.fax': " + firstNotNull_supplier_fax + " and " + supplier_fax2 + "." );
						}
						if (firstNotNull_supplier_fax == null && supplier_fax2 != null) {
							firstNotNull_supplier_fax = supplier_fax2;
						}
					}
					supplier_res.setFax(firstNotNull_supplier_fax);
					// attribute 'Supplier.homePage'
					String firstNotNull_supplier_homePage = Util.getStringValue(r.getAs("supplier_0.homePage"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_homePage2 = Util.getStringValue(r.getAs("supplier_" + i + ".homePage"));
						if (firstNotNull_supplier_homePage != null && supplier_homePage2 != null && !firstNotNull_supplier_homePage.equals(supplier_homePage2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.homePage': " + firstNotNull_supplier_homePage + " and " + supplier_homePage2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.homePage': " + firstNotNull_supplier_homePage + " and " + supplier_homePage2 + "." );
						}
						if (firstNotNull_supplier_homePage == null && supplier_homePage2 != null) {
							firstNotNull_supplier_homePage = supplier_homePage2;
						}
					}
					supplier_res.setHomePage(firstNotNull_supplier_homePage);
					// attribute 'Supplier.phone'
					String firstNotNull_supplier_phone = Util.getStringValue(r.getAs("supplier_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_phone2 = Util.getStringValue(r.getAs("supplier_" + i + ".phone"));
						if (firstNotNull_supplier_phone != null && supplier_phone2 != null && !firstNotNull_supplier_phone.equals(supplier_phone2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.phone': " + firstNotNull_supplier_phone + " and " + supplier_phone2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.phone': " + firstNotNull_supplier_phone + " and " + supplier_phone2 + "." );
						}
						if (firstNotNull_supplier_phone == null && supplier_phone2 != null) {
							firstNotNull_supplier_phone = supplier_phone2;
						}
					}
					supplier_res.setPhone(firstNotNull_supplier_phone);
					// attribute 'Supplier.postalCode'
					String firstNotNull_supplier_postalCode = Util.getStringValue(r.getAs("supplier_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_postalCode2 = Util.getStringValue(r.getAs("supplier_" + i + ".postalCode"));
						if (firstNotNull_supplier_postalCode != null && supplier_postalCode2 != null && !firstNotNull_supplier_postalCode.equals(supplier_postalCode2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.postalCode': " + firstNotNull_supplier_postalCode + " and " + supplier_postalCode2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.postalCode': " + firstNotNull_supplier_postalCode + " and " + supplier_postalCode2 + "." );
						}
						if (firstNotNull_supplier_postalCode == null && supplier_postalCode2 != null) {
							firstNotNull_supplier_postalCode = supplier_postalCode2;
						}
					}
					supplier_res.setPostalCode(firstNotNull_supplier_postalCode);
					// attribute 'Supplier.region'
					String firstNotNull_supplier_region = Util.getStringValue(r.getAs("supplier_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_region2 = Util.getStringValue(r.getAs("supplier_" + i + ".region"));
						if (firstNotNull_supplier_region != null && supplier_region2 != null && !firstNotNull_supplier_region.equals(supplier_region2)) {
							insert_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.region': " + firstNotNull_supplier_region + " and " + supplier_region2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplier_res.getId()+"]: different values found for attribute 'Supplier.region': " + firstNotNull_supplier_region + " and " + supplier_region2 + "." );
						}
						if (firstNotNull_supplier_region == null && supplier_region2 != null) {
							firstNotNull_supplier_region = supplier_region2;
						}
					}
					supplier_res.setRegion(firstNotNull_supplier_region);
					// attribute 'Product.id'
					Integer firstNotNull_product_id = Util.getIntegerValue(r.getAs("product_0.id"));
					product_res.setId(firstNotNull_product_id);
					// attribute 'Product.name'
					String firstNotNull_product_name = Util.getStringValue(r.getAs("product_0.name"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_name2 = Util.getStringValue(r.getAs("product_" + i + ".name"));
						if (firstNotNull_product_name != null && product_name2 != null && !firstNotNull_product_name.equals(product_name2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
						}
						if (firstNotNull_product_name == null && product_name2 != null) {
							firstNotNull_product_name = product_name2;
						}
					}
					product_res.setName(firstNotNull_product_name);
					// attribute 'Product.supplierRef'
					Integer firstNotNull_product_supplierRef = Util.getIntegerValue(r.getAs("product_0.supplierRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_supplierRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".supplierRef"));
						if (firstNotNull_product_supplierRef != null && product_supplierRef2 != null && !firstNotNull_product_supplierRef.equals(product_supplierRef2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
						}
						if (firstNotNull_product_supplierRef == null && product_supplierRef2 != null) {
							firstNotNull_product_supplierRef = product_supplierRef2;
						}
					}
					product_res.setSupplierRef(firstNotNull_product_supplierRef);
					// attribute 'Product.categoryRef'
					Integer firstNotNull_product_categoryRef = Util.getIntegerValue(r.getAs("product_0.categoryRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_categoryRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".categoryRef"));
						if (firstNotNull_product_categoryRef != null && product_categoryRef2 != null && !firstNotNull_product_categoryRef.equals(product_categoryRef2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
						}
						if (firstNotNull_product_categoryRef == null && product_categoryRef2 != null) {
							firstNotNull_product_categoryRef = product_categoryRef2;
						}
					}
					product_res.setCategoryRef(firstNotNull_product_categoryRef);
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_product_quantityPerUnit = Util.getStringValue(r.getAs("product_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_quantityPerUnit2 = Util.getStringValue(r.getAs("product_" + i + ".quantityPerUnit"));
						if (firstNotNull_product_quantityPerUnit != null && product_quantityPerUnit2 != null && !firstNotNull_product_quantityPerUnit.equals(product_quantityPerUnit2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
						}
						if (firstNotNull_product_quantityPerUnit == null && product_quantityPerUnit2 != null) {
							firstNotNull_product_quantityPerUnit = product_quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_product_quantityPerUnit);
					// attribute 'Product.unitPrice'
					Double firstNotNull_product_unitPrice = Util.getDoubleValue(r.getAs("product_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double product_unitPrice2 = Util.getDoubleValue(r.getAs("product_" + i + ".unitPrice"));
						if (firstNotNull_product_unitPrice != null && product_unitPrice2 != null && !firstNotNull_product_unitPrice.equals(product_unitPrice2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
						}
						if (firstNotNull_product_unitPrice == null && product_unitPrice2 != null) {
							firstNotNull_product_unitPrice = product_unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_product_unitPrice);
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_product_reorderLevel = Util.getIntegerValue(r.getAs("product_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_reorderLevel2 = Util.getIntegerValue(r.getAs("product_" + i + ".reorderLevel"));
						if (firstNotNull_product_reorderLevel != null && product_reorderLevel2 != null && !firstNotNull_product_reorderLevel.equals(product_reorderLevel2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
						}
						if (firstNotNull_product_reorderLevel == null && product_reorderLevel2 != null) {
							firstNotNull_product_reorderLevel = product_reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_product_reorderLevel);
					// attribute 'Product.discontinued'
					Boolean firstNotNull_product_discontinued = Util.getBooleanValue(r.getAs("product_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean product_discontinued2 = Util.getBooleanValue(r.getAs("product_" + i + ".discontinued"));
						if (firstNotNull_product_discontinued != null && product_discontinued2 != null && !firstNotNull_product_discontinued.equals(product_discontinued2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
						}
						if (firstNotNull_product_discontinued == null && product_discontinued2 != null) {
							firstNotNull_product_discontinued = product_discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_product_discontinued);
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_product_unitsInStock = Util.getIntegerValue(r.getAs("product_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsInStock2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsInStock"));
						if (firstNotNull_product_unitsInStock != null && product_unitsInStock2 != null && !firstNotNull_product_unitsInStock.equals(product_unitsInStock2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_product_unitsInStock + " and " + product_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_product_unitsInStock + " and " + product_unitsInStock2 + "." );
						}
						if (firstNotNull_product_unitsInStock == null && product_unitsInStock2 != null) {
							firstNotNull_product_unitsInStock = product_unitsInStock2;
						}
					}
					product_res.setUnitsInStock(firstNotNull_product_unitsInStock);
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_product_unitsOnOrder = Util.getIntegerValue(r.getAs("product_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsOnOrder2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsOnOrder"));
						if (firstNotNull_product_unitsOnOrder != null && product_unitsOnOrder2 != null && !firstNotNull_product_unitsOnOrder.equals(product_unitsOnOrder2)) {
							insert_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_product_unitsOnOrder + " and " + product_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_product_unitsOnOrder + " and " + product_unitsOnOrder2 + "." );
						}
						if (firstNotNull_product_unitsOnOrder == null && product_unitsOnOrder2 != null) {
							firstNotNull_product_unitsOnOrder = product_unitsOnOrder2;
						}
					}
					product_res.setUnitsOnOrder(firstNotNull_product_unitsOnOrder);
	
					insert_res.setSupplier(supplier_res);
					insert_res.setProduct(product_res);
					return insert_res;
		}
		, Encoders.bean(Insert.class));
	
	}
	
	//Empty arguments
	public Dataset<Insert> getInsertList(){
		 return getInsertList(null,null);
	}
	
	public abstract Dataset<Insert> getInsertList(
		Condition<SupplierAttribute> supplier_condition,
		Condition<ProductAttribute> product_condition);
	
	public Dataset<Insert> getInsertListBySupplierCondition(
		Condition<SupplierAttribute> supplier_condition
	){
		return getInsertList(supplier_condition, null);
	}
	
	public Dataset<Insert> getInsertListBySupplier(Supplier supplier) {
		Condition<SupplierAttribute> cond = null;
		cond = Condition.simple(SupplierAttribute.id, Operator.EQUALS, supplier.getId());
		Dataset<Insert> res = getInsertListBySupplierCondition(cond);
	return res;
	}
	public Dataset<Insert> getInsertListByProductCondition(
		Condition<ProductAttribute> product_condition
	){
		return getInsertList(null, product_condition);
	}
	
	public Insert getInsertByProduct(Product product) {
		Condition<ProductAttribute> cond = null;
		cond = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		Dataset<Insert> res = getInsertListByProductCondition(cond);
		List<Insert> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	
	
	public abstract void deleteInsertList(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition);
	
	public void deleteInsertListBySupplierCondition(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition
	){
		deleteInsertList(supplier_condition, null);
	}
	
	public void deleteInsertListBySupplier(pojo.Supplier supplier) {
		// TODO using id for selecting
		return;
	}
	public void deleteInsertListByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteInsertList(null, product_condition);
	}
	
	public void deleteInsertByProduct(pojo.Product product) {
		// TODO using id for selecting
		return;
	}
		
}
