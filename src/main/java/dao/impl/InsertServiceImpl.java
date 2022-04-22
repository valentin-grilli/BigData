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
		
			
			Dataset<Insert> res_insert_supplier;
			Dataset<Supplier> res_Supplier;
			
			
			//Join datasets or return 
			Dataset<Insert> res = fullOuterJoinsInsert(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Supplier> lonelySupplier = null;
			Dataset<Product> lonelyProduct = null;
			
			List<Dataset<Supplier>> lonelysupplierList = new ArrayList<Dataset<Supplier>>();
			lonelysupplierList.add(supplierService.getSupplierListInSuppliersFromMyMongoDB(supplier_condition, new MutableBoolean(false)));
			lonelySupplier = SupplierService.fullOuterJoinsSupplier(lonelysupplierList);
			if(lonelySupplier != null) {
				res = fullLeftOuterJoinBetweenInsertAndSupplier(res, lonelySupplier);
			}	
		
			List<Dataset<Product>> lonelyproductList = new ArrayList<Dataset<Product>>();
			lonelyproductList.add(productService.getProductListInProductsInfoFromRelData(product_condition, new MutableBoolean(false)));
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
