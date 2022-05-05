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
import conditions.SupplyAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ProductsTDO;
import tdo.SupplyTDO;
import conditions.ProductsAttribute;
import dao.services.ProductsService;
import tdo.SuppliersTDO;
import tdo.SupplyTDO;
import conditions.SuppliersAttribute;
import dao.services.SuppliersService;
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

public class SupplyServiceImpl extends dao.services.SupplyService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SupplyServiceImpl.class);
	
	
	// Left side 'SupplierRef' of reference [supply ]
	public Dataset<ProductsTDO> getProductsTDOListSuppliedProductInSupplyInProductsInfoFromRelDB(Condition<ProductsAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsInfoFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "ProductsInfo", where);
		
	
		Dataset<ProductsTDO> res = d.map((MapFunction<Row, ProductsTDO>) r -> {
					ProductsTDO products_res = new ProductsTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Products.ProductId]
					Integer productId = Util.getIntegerValue(r.getAs("ProductID"));
					products_res.setProductId(productId);
					
					// attribute [Products.ProductName]
					String productName = Util.getStringValue(r.getAs("ProductName"));
					products_res.setProductName(productName);
					
					// attribute [Products.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					products_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [Products.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					products_res.setUnitPrice(unitPrice);
					
					// attribute [Products.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					products_res.setReorderLevel(reorderLevel);
					
					// attribute [Products.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					products_res.setDiscontinued(discontinued);
	
					// Get reference column [SupplierRef ] for reference [supply]
					String relDB_ProductsInfo_supply_SupplierRef = r.getAs("SupplierRef") == null ? null : r.getAs("SupplierRef").toString();
					products_res.setRelDB_ProductsInfo_supply_SupplierRef(relDB_ProductsInfo_supply_SupplierRef);
	
	
					return products_res;
				}, Encoders.bean(ProductsTDO.class));
	
	
		return res;
	}
	
	// Right side 'SupplierID' of reference [supply ]
	public Dataset<SuppliersTDO> getSuppliersTDOListSupplierInSupplyInProductsInfoFromRelDB(Condition<SuppliersAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = SuppliersServiceImpl.getBSONMatchQueryInSuppliersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Suppliers", bsonQuery);
	
		Dataset<SuppliersTDO> res = dataset.flatMap((FlatMapFunction<Row, SuppliersTDO>) r -> {
				Set<SuppliersTDO> list_res = new HashSet<SuppliersTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				SuppliersTDO suppliers1 = new SuppliersTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Suppliers.supplierId for field SupplierID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("SupplierID")) {
						if(nestedRow.getAs("SupplierID") == null){
							suppliers1.setSupplierId(null);
						}else{
							suppliers1.setSupplierId(Util.getIntegerValue(nestedRow.getAs("SupplierID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.companyName for field CompanyName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName") == null){
							suppliers1.setCompanyName(null);
						}else{
							suppliers1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.contactName for field ContactName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName") == null){
							suppliers1.setContactName(null);
						}else{
							suppliers1.setContactName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.contactTitle for field ContactTitle			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactTitle")) {
						if(nestedRow.getAs("ContactTitle") == null){
							suppliers1.setContactTitle(null);
						}else{
							suppliers1.setContactTitle(Util.getStringValue(nestedRow.getAs("ContactTitle")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							suppliers1.setAddress(null);
						}else{
							suppliers1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							suppliers1.setCity(null);
						}else{
							suppliers1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							suppliers1.setRegion(null);
						}else{
							suppliers1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							suppliers1.setPostalCode(null);
						}else{
							suppliers1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							suppliers1.setCountry(null);
						}else{
							suppliers1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.phone for field Phone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone") == null){
							suppliers1.setPhone(null);
						}else{
							suppliers1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.fax for field Fax			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Fax")) {
						if(nestedRow.getAs("Fax") == null){
							suppliers1.setFax(null);
						}else{
							suppliers1.setFax(Util.getStringValue(nestedRow.getAs("Fax")));
							toAdd1 = true;					
							}
					}
					// 	attribute Suppliers.homePage for field HomePage			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePage")) {
						if(nestedRow.getAs("HomePage") == null){
							suppliers1.setHomePage(null);
						}else{
							suppliers1.setHomePage(Util.getStringValue(nestedRow.getAs("HomePage")));
							toAdd1 = true;					
							}
					}
					
						// field  SupplierID for reference supply . Reference field : SupplierID
					nestedRow =  r1;
					if(nestedRow != null) {
						suppliers1.setRelDB_ProductsInfo_supply_SupplierID(nestedRow.getAs("SupplierID") == null ? null : nestedRow.getAs("SupplierID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(suppliers1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(SuppliersTDO.class));
		res= res.dropDuplicates(new String[]{"supplierId"});
		return res;
	}
	
	
	
	
	public Dataset<Supply> getSupplyList(
		Condition<ProductsAttribute> suppliedProduct_condition,
		Condition<SuppliersAttribute> supplier_condition){
			SupplyServiceImpl supplyService = this;
			ProductsService productsService = new ProductsServiceImpl();  
			SuppliersService suppliersService = new SuppliersServiceImpl();
			MutableBoolean suppliedProduct_refilter = new MutableBoolean(false);
			List<Dataset<Supply>> datasetsPOJO = new ArrayList<Dataset<Supply>>();
			boolean all_already_persisted = false;
			MutableBoolean supplier_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'suppliedProduct' in reference 'supply'. A->B Scenario
			supplier_refilter = new MutableBoolean(false);
			Dataset<ProductsTDO> productsTDOsupplysuppliedProduct = supplyService.getProductsTDOListSuppliedProductInSupplyInProductsInfoFromRelDB(suppliedProduct_condition, suppliedProduct_refilter);
			Dataset<SuppliersTDO> suppliersTDOsupplysupplier = supplyService.getSuppliersTDOListSupplierInSupplyInProductsInfoFromRelDB(supplier_condition, supplier_refilter);
			
			Dataset<Row> res_supply_temp = productsTDOsupplysuppliedProduct.join(suppliersTDOsupplysupplier
					.withColumnRenamed("supplierId", "Suppliers_supplierId")
					.withColumnRenamed("companyName", "Suppliers_companyName")
					.withColumnRenamed("contactName", "Suppliers_contactName")
					.withColumnRenamed("contactTitle", "Suppliers_contactTitle")
					.withColumnRenamed("address", "Suppliers_address")
					.withColumnRenamed("city", "Suppliers_city")
					.withColumnRenamed("region", "Suppliers_region")
					.withColumnRenamed("postalCode", "Suppliers_postalCode")
					.withColumnRenamed("country", "Suppliers_country")
					.withColumnRenamed("phone", "Suppliers_phone")
					.withColumnRenamed("fax", "Suppliers_fax")
					.withColumnRenamed("homePage", "Suppliers_homePage")
					.withColumnRenamed("logEvents", "Suppliers_logEvents"),
					productsTDOsupplysuppliedProduct.col("relDB_ProductsInfo_supply_SupplierRef").equalTo(suppliersTDOsupplysupplier.col("relDB_ProductsInfo_supply_SupplierID")));
		
			Dataset<Supply> res_supply = res_supply_temp.map(
				(MapFunction<Row, Supply>) r -> {
					Supply res = new Supply();
					Products A = new Products();
					Suppliers B = new Suppliers();
					A.setProductId(Util.getIntegerValue(r.getAs("productId")));
					A.setProductName(Util.getStringValue(r.getAs("productName")));
					A.setQuantityPerUnit(Util.getStringValue(r.getAs("quantityPerUnit")));
					A.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					A.setUnitsInStock(Util.getIntegerValue(r.getAs("unitsInStock")));
					A.setUnitsOnOrder(Util.getIntegerValue(r.getAs("unitsOnOrder")));
					A.setReorderLevel(Util.getIntegerValue(r.getAs("reorderLevel")));
					A.setDiscontinued(Util.getBooleanValue(r.getAs("discontinued")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setSupplierId(Util.getIntegerValue(r.getAs("Suppliers_supplierId")));
					B.setCompanyName(Util.getStringValue(r.getAs("Suppliers_companyName")));
					B.setContactName(Util.getStringValue(r.getAs("Suppliers_contactName")));
					B.setContactTitle(Util.getStringValue(r.getAs("Suppliers_contactTitle")));
					B.setAddress(Util.getStringValue(r.getAs("Suppliers_address")));
					B.setCity(Util.getStringValue(r.getAs("Suppliers_city")));
					B.setRegion(Util.getStringValue(r.getAs("Suppliers_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Suppliers_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Suppliers_country")));
					B.setPhone(Util.getStringValue(r.getAs("Suppliers_phone")));
					B.setFax(Util.getStringValue(r.getAs("Suppliers_fax")));
					B.setHomePage(Util.getStringValue(r.getAs("Suppliers_homePage")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Suppliers_logEvents")));
						
					res.setSuppliedProduct(A);
					res.setSupplier(B);
					return res;
				},Encoders.bean(Supply.class)
			);
		
			datasetsPOJO.add(res_supply);
		
			
			Dataset<Supply> res_supply_suppliedProduct;
			Dataset<Products> res_Products;
			
			
			//Join datasets or return 
			Dataset<Supply> res = fullOuterJoinsSupply(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Products> lonelySuppliedProduct = null;
			Dataset<Suppliers> lonelySupplier = null;
			
			List<Dataset<Products>> lonelysuppliedProductList = new ArrayList<Dataset<Products>>();
			lonelysuppliedProductList.add(productsService.getProductsListInProductsStockInfoFromMyRedisDB(suppliedProduct_condition, new MutableBoolean(false)));
			lonelySuppliedProduct = ProductsService.fullOuterJoinsProducts(lonelysuppliedProductList);
			if(lonelySuppliedProduct != null) {
				res = fullLeftOuterJoinBetweenSupplyAndSuppliedProduct(res, lonelySuppliedProduct);
			}	
		
		
			
			if(suppliedProduct_refilter.booleanValue() || supplier_refilter.booleanValue())
				res = res.filter((FilterFunction<Supply>) r -> (suppliedProduct_condition == null || suppliedProduct_condition.evaluate(r.getSuppliedProduct())) && (supplier_condition == null || supplier_condition.evaluate(r.getSupplier())));
			
		
			return res;
		
		}
	
	public Dataset<Supply> getSupplyListBySuppliedProductCondition(
		Condition<ProductsAttribute> suppliedProduct_condition
	){
		return getSupplyList(suppliedProduct_condition, null);
	}
	
	public Supply getSupplyBySuppliedProduct(Products suppliedProduct) {
		Condition<ProductsAttribute> cond = null;
		cond = Condition.simple(ProductsAttribute.productId, Operator.EQUALS, suppliedProduct.getProductId());
		Dataset<Supply> res = getSupplyListBySuppliedProductCondition(cond);
		List<Supply> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Supply> getSupplyListBySupplierCondition(
		Condition<SuppliersAttribute> supplier_condition
	){
		return getSupplyList(null, supplier_condition);
	}
	
	public Dataset<Supply> getSupplyListBySupplier(Suppliers supplier) {
		Condition<SuppliersAttribute> cond = null;
		cond = Condition.simple(SuppliersAttribute.supplierId, Operator.EQUALS, supplier.getSupplierId());
		Dataset<Supply> res = getSupplyListBySupplierCondition(cond);
	return res;
	}
	
	public void insertSupply(Supply supply){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertSupplyInRefStructProductsInfoInMyRelDB(supply);
	}
	
	
	
	public 	boolean insertSupplyInRefStructProductsInfoInMyRelDB(Supply supply){
	 	// Rel 'supply' Insert in reference structure 'ProductsInfo'
		Products productsSuppliedProduct = supply.getSuppliedProduct();
		Suppliers suppliersSupplier = supply.getSupplier();
	
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		String filtercolumn;
		Object filtervalue;
		columns.add("SupplierRef");
		values.add(suppliersSupplier==null?null:suppliersSupplier.getSupplierId());
		filtercolumn = "ProductID";
		filtervalue = productsSuppliedProduct.getProductId();
		DBConnectionMgr.updateInTable(filtercolumn, filtervalue, columns, values, "ProductsInfo", "myRelDB");					
		return true;
	}
	
	
	
	
	public void deleteSupplyList(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition){
			//TODO
		}
	
	public void deleteSupplyListBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition
	){
		deleteSupplyList(suppliedProduct_condition, null);
	}
	
	public void deleteSupplyBySuppliedProduct(pojo.Products suppliedProduct) {
		// TODO using id for selecting
		return;
	}
	public void deleteSupplyListBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition
	){
		deleteSupplyList(null, supplier_condition);
	}
	
	public void deleteSupplyListBySupplier(pojo.Suppliers supplier) {
		// TODO using id for selecting
		return;
	}
		
}
