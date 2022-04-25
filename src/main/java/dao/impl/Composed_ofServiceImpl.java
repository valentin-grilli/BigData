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
import conditions.Composed_ofAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrderTDO;
import tdo.Composed_ofTDO;
import conditions.OrderAttribute;
import dao.services.OrderService;
import tdo.ProductTDO;
import tdo.Composed_ofTDO;
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

public class Composed_ofServiceImpl extends dao.services.Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofServiceImpl.class);
	// A<-AB->B . getAListInREL
	//join structure
	// Left side 'OrderRef' of reference [orderR ]
	public Dataset<OrderTDO> getOrderTDOListOrderInOrderRInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrderTDO> res = dataset.flatMap((FlatMapFunction<Row, OrderTDO>) r -> {
				Set<OrderTDO> list_res = new HashSet<OrderTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrderTDO order1 = new OrderTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							order1.setFreight(null);
						}else{
							order1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							order1.setOrderDate(null);
						}else{
							order1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							order1.setRequiredDate(null);
						}else{
							order1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							order1.setShipAddress(null);
						}else{
							order1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							order1.setId(null);
						}else{
							order1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							order1.setShipCity(null);
						}else{
							order1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							order1.setShipCountry(null);
						}else{
							order1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							order1.setShipName(null);
						}else{
							order1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							order1.setShipPostalCode(null);
						}else{
							order1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							order1.setShipRegion(null);
						}else{
							order1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							order1.setShippedDate(null);
						}else{
							order1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					
						// field  OrderID for reference orderR . Reference field : OrderID
					nestedRow =  r1;
					if(nestedRow != null) {
						order1.setRelSchema_Order_Details_orderR_OrderID(nestedRow.getAs("OrderID") == null ? null : nestedRow.getAs("OrderID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(order1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrderTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInOrder_DetailsFromRelData(Condition<Composed_ofAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInOrder_DetailsFromRelDataWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInOrder_DetailsFromRelData(conditions.SetClause<Composed_ofAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<Composed_ofAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<Composed_ofAttribute, Object> e : clause.entrySet()) {
				Composed_ofAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == Composed_ofAttribute.unitPrice ) {
					res.add("UnitPrice = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == Composed_ofAttribute.quantity ) {
					res.add("Quantity = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == Composed_ofAttribute.discount ) {
					res.add("Discount = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInOrder_DetailsFromRelDataWithTableAlias(Condition<Composed_ofAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				Composed_ofAttribute attr = ((SimpleCondition<Composed_ofAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<Composed_ofAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<Composed_ofAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == Composed_ofAttribute.unitPrice ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "UnitPrice " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == Composed_ofAttribute.quantity ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Quantity " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == Composed_ofAttribute.discount ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Discount " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == Composed_ofAttribute.unitPrice ) {
						if(op == Operator.EQUALS)
							where =  "UnitPrice IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "UnitPrice IS NOT NULL";
					}
					if(attr == Composed_ofAttribute.quantity ) {
						if(op == Operator.EQUALS)
							where =  "Quantity IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Quantity IS NOT NULL";
					}
					if(attr == Composed_ofAttribute.discount ) {
						if(op == Operator.EQUALS)
							where =  "Discount IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Discount IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInOrder_DetailsFromRelData(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInOrder_DetailsFromRelData(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInOrder_DetailsFromRelData(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInOrder_DetailsFromRelData(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	
	
	
	// A<-AB->B . getBListInREL
	
	
	
	public Dataset<Composed_ofTDO> getComposed_ofTDOListInProductsInfoAndOrder_DetailsFromrelData(Condition<ProductAttribute> product_cond, Condition<Composed_ofAttribute> composed_of_cond, MutableBoolean refilterFlag, MutableBoolean composed_of_refilter) {
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromRelDataWithTableAlias(product_cond, refilterFlag, "ProductsInfo.");
		Pair<String, List<String>> whereClause2 = Composed_ofServiceImpl.getSQLWhereClauseInOrder_DetailsFromRelDataWithTableAlias(composed_of_cond, composed_of_refilter, "Order_Details.");
		
		String where1 = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where1 = where1.replaceFirst("\\?", preparedValue);
		}
	
		String where2 = whereClause2.getKey();
		preparedValues = whereClause2.getValue();
		for(String preparedValue : preparedValues) {
			where2 = where2.replaceFirst("\\?", preparedValue);
		}
		
		String where = "";
		if(where1 != null)
			where = " AND " + where1;
		if(where2 != null)
			where = " AND " + where2;
	
		String aliasedColumns = "ProductsInfo.ProductID as ProductsInfo_ProductID,ProductsInfo.ProductName as ProductsInfo_ProductName,ProductsInfo.SupplierRef as ProductsInfo_SupplierRef,ProductsInfo.CategoryRef as ProductsInfo_CategoryRef,ProductsInfo.QuantityPerUnit as ProductsInfo_QuantityPerUnit,ProductsInfo.UnitPrice as ProductsInfo_UnitPrice,ProductsInfo.ReorderLevel as ProductsInfo_ReorderLevel,ProductsInfo.Discontinued as ProductsInfo_Discontinued, Order_Details.OrderRef as Order_Details_OrderRef,Order_Details.ProductRef as Order_Details_ProductRef,Order_Details.UnitPrice as Order_Details_UnitPrice,Order_Details.Quantity as Order_Details_Quantity,Order_Details.Discount as Order_Details_Discount";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "(SELECT " + aliasedColumns + " FROM ProductsInfo, Order_Details WHERE Order_Details.ProductRef = ProductsInfo.ProductID" + where + ") AS JOIN_TABLE", null);
		Dataset<Composed_ofTDO> res = d.map((MapFunction<Row, Composed_ofTDO>) r -> {
					Composed_ofTDO composed_of_res = new Composed_ofTDO();
					composed_of_res.setProduct(new Product());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Composed_of.UnitPrice]
					Double composed_of_unitPrice = Util.getDoubleValue(r.getAs("Order_Details_UnitPrice"));
					composed_of_res.setUnitPrice(composed_of_unitPrice);
					
					// attribute [Composed_of.Quantity]
					Integer composed_of_quantity = Util.getIntegerValue(r.getAs("Order_Details_Quantity"));
					composed_of_res.setQuantity(composed_of_quantity);
					
					// attribute [Composed_of.Discount]
					Double composed_of_discount = Util.getDoubleValue(r.getAs("Order_Details_Discount"));
					composed_of_res.setDiscount(composed_of_discount);
		
		
					
					// attribute [Product.Id]
					Integer product_id = Util.getIntegerValue(r.getAs("ProductsInfo_ProductID"));
					composed_of_res.getProduct().setId(product_id);
					
					// attribute [Product.Name]
					String product_name = Util.getStringValue(r.getAs("ProductsInfo_ProductName"));
					composed_of_res.getProduct().setName(product_name);
					
					// attribute [Product.SupplierRef]
					Integer product_supplierRef = Util.getIntegerValue(r.getAs("ProductsInfo_SupplierRef"));
					composed_of_res.getProduct().setSupplierRef(product_supplierRef);
					
					// attribute [Product.CategoryRef]
					Integer product_categoryRef = Util.getIntegerValue(r.getAs("ProductsInfo_CategoryRef"));
					composed_of_res.getProduct().setCategoryRef(product_categoryRef);
					
					// attribute [Product.QuantityPerUnit]
					String product_quantityPerUnit = Util.getStringValue(r.getAs("ProductsInfo_QuantityPerUnit"));
					composed_of_res.getProduct().setQuantityPerUnit(product_quantityPerUnit);
					
					// attribute [Product.UnitPrice]
					Double product_unitPrice = Util.getDoubleValue(r.getAs("ProductsInfo_UnitPrice"));
					composed_of_res.getProduct().setUnitPrice(product_unitPrice);
					
					// attribute [Product.ReorderLevel]
					Integer product_reorderLevel = Util.getIntegerValue(r.getAs("ProductsInfo_ReorderLevel"));
					composed_of_res.getProduct().setReorderLevel(product_reorderLevel);
					
					// attribute [Product.Discontinued]
					Boolean product_discontinued = Util.getBooleanValue(r.getAs("ProductsInfo_Discontinued"));
					composed_of_res.getProduct().setDiscontinued(product_discontinued);
					
					String orderR_OrderRef = r.getAs("Order_Details_OrderRef") == null ? null : r.getAs("Order_Details_OrderRef").toString();
					composed_of_res.setRelSchema_Order_Details_orderR_OrderRef(orderR_OrderRef);
					
		
					return composed_of_res;
				}, Encoders.bean(Composed_ofTDO.class));
		
		return res;
	}
	
	
	
	
	
	
	public Dataset<Composed_of> getComposed_ofList(
		Condition<OrderAttribute> order_condition,
		Condition<ProductAttribute> product_condition,
		Condition<Composed_ofAttribute> composed_of_condition
	){
		Composed_ofServiceImpl composed_ofService = this;
		OrderService orderService = new OrderServiceImpl();  
		ProductService productService = new ProductServiceImpl();
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Composed_of>> datasetsPOJO = new ArrayList<Dataset<Composed_of>>();
		boolean all_already_persisted = false;
		MutableBoolean product_refilter = new MutableBoolean(false);
		MutableBoolean composed_of_refilter = new MutableBoolean(false);
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		//join between 2 SQL tables and a non-relational structure
		// (A) (AB - B)
		product_refilter = new MutableBoolean(false);
		Dataset<Composed_ofTDO> res_composed_of_productR_orderR = composed_ofService.getComposed_ofTDOListInProductsInfoAndOrder_DetailsFromrelData(product_condition, composed_of_condition, product_refilter, composed_of_refilter);
		Dataset<OrderTDO> res_orderR_productR = composed_ofService.getOrderTDOListOrderInOrderRInOrdersFromMongoSchema(order_condition, order_refilter);
		Dataset<Row> B_res_orderR_productR = res_orderR_productR
			.withColumnRenamed("id", "B_id")
			.withColumnRenamed("freight", "B_freight")
			.withColumnRenamed("orderDate", "B_orderDate")
			.withColumnRenamed("requiredDate", "B_requiredDate")
			.withColumnRenamed("shipAddress", "B_shipAddress")
			.withColumnRenamed("shipCity", "B_shipCity")
			.withColumnRenamed("shipCountry", "B_shipCountry")
			.withColumnRenamed("shipName", "B_shipName")
			.withColumnRenamed("shipPostalCode", "B_shipPostalCode")
			.withColumnRenamed("shipRegion", "B_shipRegion")
			.withColumnRenamed("shippedDate", "B_shippedDate")
			.withColumnRenamed("logEvents", "B_logEvents");
		
		Dataset<Row> res_row_productR_orderR = res_composed_of_productR_orderR.join(B_res_orderR_productR,
			res_composed_of_productR_orderR.col("relSchema_Order_Details_orderR_OrderRef").equalTo(B_res_orderR_productR.col("relSchema_Order_Details_orderR_OrderID")));
		Dataset<Composed_of> res_Order_orderR = res_row_productR_orderR.map((MapFunction<Row, Composed_of>) r -> {
					Composed_of res = new Composed_of();
					
					Order B = new Order();
					Product A = new Product();
						
					Object o = r.getAs("product");
					if(o != null) {
						if(o instanceof Row) {
							Row r2 = (Row) o;
							A.setId(Util.getIntegerValue(r2.getAs("id")));
							A.setName(Util.getStringValue(r2.getAs("name")));
							A.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
							A.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
							A.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
							A.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
							A.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
							A.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
							A.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
							A.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
							A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r2.getAs("logEvents")));
						}
						if(o instanceof Product)
							A = (Product) o;
					}
		
		
					res.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					res.setQuantity(Util.getIntegerValue(r.getAs("quantity")));
					res.setDiscount(Util.getDoubleValue(r.getAs("discount")));
					res.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("B_id")));
					B.setFreight(Util.getDoubleValue(r.getAs("B_freight")));
					B.setOrderDate(Util.getLocalDateValue(r.getAs("B_orderDate")));
					B.setRequiredDate(Util.getLocalDateValue(r.getAs("B_requiredDate")));
					B.setShipAddress(Util.getStringValue(r.getAs("B_shipAddress")));
					B.setShipCity(Util.getStringValue(r.getAs("B_shipCity")));
					B.setShipCountry(Util.getStringValue(r.getAs("B_shipCountry")));
					B.setShipName(Util.getStringValue(r.getAs("B_shipName")));
					B.setShipPostalCode(Util.getStringValue(r.getAs("B_shipPostalCode")));
					B.setShipRegion(Util.getStringValue(r.getAs("B_shipRegion")));
					B.setShippedDate(Util.getLocalDateValue(r.getAs("B_shippedDate")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("B_logEvents")));
						
					res.setProduct(A);
					res.setOrder(B);
					return res;
				}, Encoders.bean(Composed_of.class));
		
		datasetsPOJO.add(res_Order_orderR);
		
	
		
		Dataset<Composed_of> res_composed_of_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Composed_of> res = fullOuterJoinsComposed_of(datasetsPOJO);
		if(res == null)
			return null;
	
		Dataset<Order> lonelyOrder = null;
		Dataset<Product> lonelyProduct = null;
		
	
		List<Dataset<Product>> lonelyproductList = new ArrayList<Dataset<Product>>();
		lonelyproductList.add(productService.getProductListInStockInfoPairsFromRedisDB(product_condition, new MutableBoolean(false)));
		lonelyProduct = ProductService.fullOuterJoinsProduct(lonelyproductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinBetweenComposed_ofAndProduct(res, lonelyProduct);
		}	
	
		
		if(order_refilter.booleanValue() || product_refilter.booleanValue() || composed_of_refilter.booleanValue())
			res = res.filter((FilterFunction<Composed_of>) r -> (order_condition == null || order_condition.evaluate(r.getOrder())) && (product_condition == null || product_condition.evaluate(r.getProduct())) && (composed_of_condition == null || composed_of_condition.evaluate(r)));
		
	
		return res;
	
	}
	
	public Dataset<Composed_of> getComposed_ofListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getComposed_ofList(order_condition, null, null);
	}
	
	public Dataset<Composed_of> getComposed_ofListByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Composed_of> res = getComposed_ofListByOrderCondition(cond);
	return res;
	}
	public Dataset<Composed_of> getComposed_ofListByProductCondition(
		Condition<ProductAttribute> product_condition
	){
		return getComposed_ofList(null, product_condition, null);
	}
	
	public Dataset<Composed_of> getComposed_ofListByProduct(Product product) {
		Condition<ProductAttribute> cond = null;
		cond = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		Dataset<Composed_of> res = getComposed_ofListByProductCondition(cond);
	return res;
	}
	
	public Dataset<Composed_of> getComposed_ofListByComposed_ofCondition(
		Condition<Composed_ofAttribute> composed_of_condition
	){
		return getComposed_ofList(null, null, composed_of_condition);
	}
	
	public void insertComposed_of(Composed_of composed_of){
		//Link entities in join structures.
		insertComposed_ofInJoinStructOrder_DetailsInRelData(composed_of);
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	public 	boolean insertComposed_ofInJoinStructOrder_DetailsInRelData(Composed_of composed_of){
	 	// Rel 'composed_of' Insert in join structure 'Order_Details'
		
		Order order_order = composed_of.getOrder();
		Product product_product = composed_of.getProduct();
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		List<List<Object>> rows = new ArrayList<>();
		columns.add("UnitPrice");
		values.add(composed_of.getUnitPrice());
		columns.add("Quantity");
		values.add(composed_of.getQuantity());
		columns.add("Discount");
		values.add(composed_of.getDiscount());
		// Role in join structure 
		columns.add("OrderRef");
		Object orderId = order_order.getId();
		values.add(orderId);
		// Role in join structure 
		columns.add("ProductRef");
		Object productId = product_product.getId();
		values.add(productId);
		rows.add(values);
		DBConnectionMgr.insertInTable(columns, rows, "Order_Details", "relData"); 					
		return true;
	
	}
	
	
	
	
	
	public void updateComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		//TODO
	}
	
	public void updateComposed_ofListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		updateComposed_ofList(order_condition, null, null, set);
	}
	
	public void updateComposed_ofListByOrder(pojo.Order order, conditions.SetClause<conditions.Composed_ofAttribute> set) {
		// TODO using id for selecting
		return;
	}
	public void updateComposed_ofListByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		updateComposed_ofList(null, product_condition, null, set);
	}
	
	public void updateComposed_ofListByProduct(pojo.Product product, conditions.SetClause<conditions.Composed_ofAttribute> set) {
		// TODO using id for selecting
		return;
	}
	
	public void updateComposed_ofListByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		updateComposed_ofList(null, null, composed_of_condition, set);
	}
	
	public void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition){
			//TODO
		}
	
	public void deleteComposed_ofListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteComposed_ofList(order_condition, null, null);
	}
	
	public void deleteComposed_ofListByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteComposed_ofListByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteComposed_ofList(null, product_condition, null);
	}
	
	public void deleteComposed_ofListByProduct(pojo.Product product) {
		// TODO using id for selecting
		return;
	}
	
	public void deleteComposed_ofListByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		deleteComposed_ofList(null, null, composed_of_condition);
	}
		
}
