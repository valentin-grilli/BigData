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
import conditions.InsertAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.SupplierTDO;
import tdo.InsertTDO;
import conditions.SupplierAttribute;
import dao.services.SupplierService;
import tdo.ProductTDO;
import tdo.InsertTDO;
import conditions.ProductAttribute;
import dao.services.ProductService;
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

public class InsertServiceImpl extends dao.services.InsertService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertServiceImpl.class);
	
	
	// Left side 'SupplierRef' of reference [supplierR ]
	public Dataset<ProductTDO> getProductTDOListProductInSupplierRInProductsInfoFromRelSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "ProductsInfo", where);
		
	
		Dataset<ProductTDO> res = d.map((MapFunction<Row, ProductTDO>) r -> {
					ProductTDO product_res = new ProductTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Product.Id]
					Integer id = Util.getIntegerValue(r.getAs("ProductID"));
					product_res.setId(id);
					
					// attribute [Product.Name]
					String name = Util.getStringValue(r.getAs("ProductName"));
					product_res.setName(name);
					
					// attribute [Product.SupplierRef]
					Integer supplierRef = Util.getIntegerValue(r.getAs("SupplierRef"));
					product_res.setSupplierRef(supplierRef);
					
					// attribute [Product.CategoryRef]
					Integer categoryRef = Util.getIntegerValue(r.getAs("CategoryRef"));
					product_res.setCategoryRef(categoryRef);
					
					// attribute [Product.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					product_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [Product.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					product_res.setUnitPrice(unitPrice);
					
					// attribute [Product.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					product_res.setReorderLevel(reorderLevel);
					
					// attribute [Product.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					product_res.setDiscontinued(discontinued);
	
					// Get reference column [SupplierRef ] for reference [supplierR]
					String relSchema_ProductsInfo_supplierR_SupplierRef = r.getAs("SupplierRef") == null ? null : r.getAs("SupplierRef").toString();
					product_res.setRelSchema_ProductsInfo_supplierR_SupplierRef(relSchema_ProductsInfo_supplierR_SupplierRef);
	
	
					return product_res;
				}, Encoders.bean(ProductTDO.class));
	
	
		return res;
	}
	
	// Right side 'SupplierID' of reference [supplierR ]
	public Dataset<SupplierTDO> getSupplierTDOListSupplierInSupplierRInProductsInfoFromRelSchema(Condition<SupplierAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = SupplierServiceImpl.getBSONMatchQueryInSuppliersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Suppliers", bsonQuery);
	
		Dataset<SupplierTDO> res = dataset.flatMap((FlatMapFunction<Row, SupplierTDO>) r -> {
				Set<SupplierTDO> list_res = new HashSet<SupplierTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				SupplierTDO supplier1 = new SupplierTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Supplier.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							supplier1.setAddress(null);
						}else{
							supplier1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							supplier1.setCity(null);
						}else{
							supplier1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.companyName for field CompanyName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName") == null){
							supplier1.setCompanyName(null);
						}else{
							supplier1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.contactName for field contactName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("contactName")) {
						if(nestedRow.getAs("contactName") == null){
							supplier1.setContactName(null);
						}else{
							supplier1.setContactName(Util.getStringValue(nestedRow.getAs("contactName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.contactTitle for field ContactTitle			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactTitle")) {
						if(nestedRow.getAs("ContactTitle") == null){
							supplier1.setContactTitle(null);
						}else{
							supplier1.setContactTitle(Util.getStringValue(nestedRow.getAs("ContactTitle")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							supplier1.setCountry(null);
						}else{
							supplier1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.fax for field Fax			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Fax")) {
						if(nestedRow.getAs("Fax") == null){
							supplier1.setFax(null);
						}else{
							supplier1.setFax(Util.getStringValue(nestedRow.getAs("Fax")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.homePage for field HomePage			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePage")) {
						if(nestedRow.getAs("HomePage") == null){
							supplier1.setHomePage(null);
						}else{
							supplier1.setHomePage(Util.getStringValue(nestedRow.getAs("HomePage")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.phone for field Phone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone") == null){
							supplier1.setPhone(null);
						}else{
							supplier1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							supplier1.setPostalCode(null);
						}else{
							supplier1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							supplier1.setRegion(null);
						}else{
							supplier1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.id for field SupplierID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("SupplierID")) {
						if(nestedRow.getAs("SupplierID") == null){
							supplier1.setId(null);
						}else{
							supplier1.setId(Util.getIntegerValue(nestedRow.getAs("SupplierID")));
							toAdd1 = true;					
							}
					}
					
						// field  SupplierID for reference supplierR . Reference field : SupplierID
					nestedRow =  r1;
					if(nestedRow != null) {
						supplier1.setRelSchema_ProductsInfo_supplierR_SupplierID(nestedRow.getAs("SupplierID") == null ? null : nestedRow.getAs("SupplierID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(supplier1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(SupplierTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	
	public Dataset<Insert> getInsertList(
		Condition<SupplierAttribute> supplier_condition,
		Condition<ProductAttribute> product_condition){
			InsertServiceImpl insertService = this;
			SupplierService supplierService = new SupplierServiceImpl();  
			ProductService productService = new ProductServiceImpl();
			MutableBoolean supplier_refilter = new MutableBoolean(false);
			List<Dataset<Insert>> datasetsPOJO = new ArrayList<Dataset<Insert>>();
			boolean all_already_persisted = false;
			MutableBoolean product_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			product_refilter = new MutableBoolean(false);
			// For role 'product' in reference 'supplierR'  B->A Scenario
			Dataset<ProductTDO> productTDOsupplierRproduct = insertService.getProductTDOListProductInSupplierRInProductsInfoFromRelSchema(product_condition, product_refilter);
			Dataset<SupplierTDO> supplierTDOsupplierRsupplier = insertService.getSupplierTDOListSupplierInSupplierRInProductsInfoFromRelSchema(supplier_condition, supplier_refilter);
			
			Dataset<Row> res_supplierR_temp = 
				supplierTDOsupplierRsupplier.join(productTDOsupplierRproduct
					.withColumnRenamed("id", "Product_id")
					.withColumnRenamed("name", "Product_name")
					.withColumnRenamed("supplierRef", "Product_supplierRef")
					.withColumnRenamed("categoryRef", "Product_categoryRef")
					.withColumnRenamed("quantityPerUnit", "Product_quantityPerUnit")
					.withColumnRenamed("unitPrice", "Product_unitPrice")
					.withColumnRenamed("reorderLevel", "Product_reorderLevel")
					.withColumnRenamed("discontinued", "Product_discontinued")
					.withColumnRenamed("unitsInStock", "Product_unitsInStock")
					.withColumnRenamed("unitsOnOrder", "Product_unitsOnOrder")
					.withColumnRenamed("logEvents", "Product_logEvents"),
					supplierTDOsupplierRsupplier.col("relSchema_ProductsInfo_supplierR_SupplierID").equalTo(productTDOsupplierRproduct.col("relSchema_ProductsInfo_supplierR_SupplierRef")));
		
			Dataset<Insert> res_supplierR = res_supplierR_temp.map(
				(MapFunction<Row, Insert>) r -> {
					Insert res = new Insert();
					Supplier A = new Supplier();
					Product B = new Product();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setAddress(Util.getStringValue(r.getAs("address")));
					A.setCity(Util.getStringValue(r.getAs("city")));
					A.setCompanyName(Util.getStringValue(r.getAs("companyName")));
					A.setContactName(Util.getStringValue(r.getAs("contactName")));
					A.setContactTitle(Util.getStringValue(r.getAs("contactTitle")));
					A.setCountry(Util.getStringValue(r.getAs("country")));
					A.setFax(Util.getStringValue(r.getAs("fax")));
					A.setHomePage(Util.getStringValue(r.getAs("homePage")));
					A.setPhone(Util.getStringValue(r.getAs("phone")));
					A.setPostalCode(Util.getStringValue(r.getAs("postalCode")));
					A.setRegion(Util.getStringValue(r.getAs("region")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("Product_id")));
					B.setName(Util.getStringValue(r.getAs("Product_name")));
					B.setSupplierRef(Util.getIntegerValue(r.getAs("Product_supplierRef")));
					B.setCategoryRef(Util.getIntegerValue(r.getAs("Product_categoryRef")));
					B.setQuantityPerUnit(Util.getStringValue(r.getAs("Product_quantityPerUnit")));
					B.setUnitPrice(Util.getDoubleValue(r.getAs("Product_unitPrice")));
					B.setReorderLevel(Util.getIntegerValue(r.getAs("Product_reorderLevel")));
					B.setDiscontinued(Util.getBooleanValue(r.getAs("Product_discontinued")));
					B.setUnitsInStock(Util.getIntegerValue(r.getAs("Product_unitsInStock")));
					B.setUnitsOnOrder(Util.getIntegerValue(r.getAs("Product_unitsOnOrder")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Product_logEvents")));
						
					res.setSupplier(A);
					res.setProduct(B);
					return res;
				},Encoders.bean(Insert.class)
			);
		
					
			datasetsPOJO.add(res_supplierR);
			
			Dataset<Insert> res_insert_supplier;
			Dataset<Supplier> res_Supplier;
			
			
			//Join datasets or return 
			Dataset<Insert> res = fullOuterJoinsInsert(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Supplier> lonelySupplier = null;
			Dataset<Product> lonelyProduct = null;
			
		
			List<Dataset<Product>> lonelyproductList = new ArrayList<Dataset<Product>>();
			lonelyproductList.add(productService.getProductListInStockInfoPairsFromRedisDB(product_condition, new MutableBoolean(false)));
			lonelyProduct = ProductService.fullOuterJoinsProduct(lonelyproductList);
			if(lonelyProduct != null) {
				res = fullLeftOuterJoinBetweenInsertAndProduct(res, lonelyProduct);
			}	
		
			
			if(supplier_refilter.booleanValue() || product_refilter.booleanValue())
				res = res.filter((FilterFunction<Insert>) r -> (supplier_condition == null || supplier_condition.evaluate(r.getSupplier())) && (product_condition == null || product_condition.evaluate(r.getProduct())));
			
		
			return res;
		
		}
	
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
	
	
	
	public void deleteInsertList(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition){
			//TODO
		}
	
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
