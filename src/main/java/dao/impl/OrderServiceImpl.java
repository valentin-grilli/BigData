package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Order;
import conditions.*;
import dao.services.OrderService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Dataset;
import org.apache.spark.sql.Encoders;
import util.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class OrderServiceImpl extends OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInOrdersFromMyMongoDB(conditions.SetClause<OrderAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<OrderAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<OrderAttribute, Object> e : clause.entrySet()) {
				OrderAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == OrderAttribute.id ) {
					String fieldName = "OrderID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.freight ) {
					String fieldName = "Freight";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.orderDate ) {
					String fieldName = "OrderDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.requiredDate ) {
					String fieldName = "RequiredDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipAddress ) {
					String fieldName = "ShipAddress";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipCity ) {
					String fieldName = "ShipCity";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipCountry ) {
					String fieldName = "ShipCountry";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipName ) {
					String fieldName = "ShipName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipPostalCode ) {
					String fieldName = "ShipPostalCode";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipRegion ) {
					String fieldName = "ShipRegion";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shippedDate ) {
					String fieldName = "ShippedDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return new ImmutablePair<List<String>, List<String>>(res, new ArrayList<String>(arrayFields));
	}
	
	public static String getBSONMatchQueryInOrdersFromMyMongoDB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				OrderAttribute attr = ((SimpleCondition<OrderAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrderAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrderAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrderAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "OrderID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.freight ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Freight': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.orderDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "OrderDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.requiredDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "RequiredDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipAddress ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipAddress': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipCity ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipCity': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipCountry ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipCountry': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipPostalCode ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipPostalCode': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipRegion ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipRegion': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shippedDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShippedDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(Condition<OrderAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				OrderAttribute attr = ((SimpleCondition<OrderAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrderAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrderAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrderAttribute.id ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "OrderID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.freight ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Freight': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.orderDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "OrderDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.requiredDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "RequiredDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipAddress ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipAddress': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipCity ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipCity': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipCountry ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipCountry': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipPostalCode ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipPostalCode': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipRegion ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipRegion': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shippedDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShippedDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
					}
					
				}
	
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $and: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight);
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $or: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight); // can be a problem
			}
		}
	
		return new ImmutablePair<String, List<String>>(query, arrayFilters);
	}
	
	
	
	public Dataset<Order> getOrderListInOrdersFromMyMongoDB(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<Order> res = dataset.flatMap((FlatMapFunction<Row, Order>) r -> {
				Set<Order> list_res = new HashSet<Order>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Order order1 = new Order();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							order1.setFreight(null);
						else{
							order1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							order1.setOrderDate(null);
						else{
							order1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							order1.setRequiredDate(null);
						else{
							order1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							order1.setShipAddress(null);
						else{
							order1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							order1.setId(null);
						else{
							order1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							order1.setShipCity(null);
						else{
							order1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							order1.setShipCountry(null);
						else{
							order1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							order1.setShipName(null);
						else{
							order1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							order1.setShipPostalCode(null);
						else{
							order1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							order1.setShipRegion(null);
						else{
							order1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							order1.setShippedDate(null);
						else{
							order1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(order1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Order.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Order> getOrderListInMake_by(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Customer> all = null;
		boolean all_already_persisted = false;
		MutableBoolean client_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'order' in reference 'customerRef'. A->B Scenario
		client_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOcustomerReforder = make_byService.getOrderTDOListOrderInCustomerRefInOrdersFromMongoSchema(order_condition, order_refilter);
		Dataset<CustomerTDO> customerTDOcustomerRefclient = make_byService.getCustomerTDOListClientInCustomerRefInOrdersFromMongoSchema(client_condition, client_refilter);
		if(client_refilter.booleanValue()) {
			if(all == null)
				all = new CustomerServiceImpl().getCustomerList(client_condition);
			joinCondition = null;
			joinCondition = customerTDOcustomerRefclient.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				customerTDOcustomerRefclient = customerTDOcustomerRefclient.as("A").join(all).select("A.*").as(Encoders.bean(CustomerTDO.class));
			else
				customerTDOcustomerRefclient = customerTDOcustomerRefclient.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CustomerTDO.class));
		}
	
		
		Dataset<Row> res_customerRef = orderTDOcustomerReforder.join(customerTDOcustomerRefclient
				.withColumnRenamed("id", "Customer_id")
				.withColumnRenamed("city", "Customer_city")
				.withColumnRenamed("companyName", "Customer_companyName")
				.withColumnRenamed("contactName", "Customer_contactName")
				.withColumnRenamed("contactTitle", "Customer_contactTitle")
				.withColumnRenamed("country", "Customer_country")
				.withColumnRenamed("fax", "Customer_fax")
				.withColumnRenamed("phone", "Customer_phone")
				.withColumnRenamed("postalCode", "Customer_postalCode")
				.withColumnRenamed("region", "Customer_region")
				.withColumnRenamed("address", "Customer_address")
				.withColumnRenamed("logEvents", "Customer_logEvents"),
				// Multi valued reference
				functions.array_contains(orderTDOcustomerReforder.col("mongoSchema_Orders_customerRef_CustomerID"),customerTDOcustomerRefclient.col("mongoSchema_Orders_customerRef_ID")));
		Dataset<Order> res_Order_customerRef = res_customerRef.select( "id", "freight", "orderDate", "requiredDate", "shipAddress", "shipCity", "shipCountry", "shipName", "shipPostalCode", "shipRegion", "shippedDate", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_customerRef = res_Order_customerRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_customerRef);
		
		
		Dataset<Make_by> res_make_by_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInShip_via(conditions.Condition<conditions.ShipperAttribute> shipper_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Shipper> all = null;
		boolean all_already_persisted = false;
		MutableBoolean shipper_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'order' in reference 'shipperRef'. A->B Scenario
		shipper_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOshipperReforder = ship_viaService.getOrderTDOListOrderInShipperRefInOrdersFromMongoSchema(order_condition, order_refilter);
		Dataset<ShipperTDO> shipperTDOshipperRefshipper = ship_viaService.getShipperTDOListShipperInShipperRefInOrdersFromMongoSchema(shipper_condition, shipper_refilter);
		if(shipper_refilter.booleanValue()) {
			if(all == null)
				all = new ShipperServiceImpl().getShipperList(shipper_condition);
			joinCondition = null;
			joinCondition = shipperTDOshipperRefshipper.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				shipperTDOshipperRefshipper = shipperTDOshipperRefshipper.as("A").join(all).select("A.*").as(Encoders.bean(ShipperTDO.class));
			else
				shipperTDOshipperRefshipper = shipperTDOshipperRefshipper.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ShipperTDO.class));
		}
	
		
		Dataset<Row> res_shipperRef = orderTDOshipperReforder.join(shipperTDOshipperRefshipper
				.withColumnRenamed("id", "Shipper_id")
				.withColumnRenamed("companyName", "Shipper_companyName")
				.withColumnRenamed("phone", "Shipper_phone")
				.withColumnRenamed("logEvents", "Shipper_logEvents"),
				orderTDOshipperReforder.col("mongoSchema_Orders_shipperRef_ShipVia").equalTo(shipperTDOshipperRefshipper.col("mongoSchema_Orders_shipperRef_ShipperID")));
		Dataset<Order> res_Order_shipperRef = res_shipperRef.select( "id", "freight", "orderDate", "requiredDate", "shipAddress", "shipCity", "shipCountry", "shipName", "shipPostalCode", "shipRegion", "shippedDate", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_shipperRef = res_Order_shipperRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_shipperRef);
		
		
		Dataset<Ship_via> res_ship_via_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInHandle(conditions.Condition<conditions.EmployeeAttribute> employee_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Employee> all = null;
		boolean all_already_persisted = false;
		MutableBoolean employee_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'order' in reference 'employeeRef'. A->B Scenario
		employee_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOemployeeReforder = handleService.getOrderTDOListOrderInEmployeeRefInOrdersFromMongoSchema(order_condition, order_refilter);
		Dataset<EmployeeTDO> employeeTDOemployeeRefemployee = handleService.getEmployeeTDOListEmployeeInEmployeeRefInOrdersFromMongoSchema(employee_condition, employee_refilter);
		if(employee_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeeServiceImpl().getEmployeeList(employee_condition);
			joinCondition = null;
			joinCondition = employeeTDOemployeeRefemployee.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				employeeTDOemployeeRefemployee = employeeTDOemployeeRefemployee.as("A").join(all).select("A.*").as(Encoders.bean(EmployeeTDO.class));
			else
				employeeTDOemployeeRefemployee = employeeTDOemployeeRefemployee.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(EmployeeTDO.class));
		}
	
		
		Dataset<Row> res_employeeRef = orderTDOemployeeReforder.join(employeeTDOemployeeRefemployee
				.withColumnRenamed("id", "Employee_id")
				.withColumnRenamed("address", "Employee_address")
				.withColumnRenamed("birthDate", "Employee_birthDate")
				.withColumnRenamed("city", "Employee_city")
				.withColumnRenamed("country", "Employee_country")
				.withColumnRenamed("extension", "Employee_extension")
				.withColumnRenamed("firstname", "Employee_firstname")
				.withColumnRenamed("hireDate", "Employee_hireDate")
				.withColumnRenamed("homePhone", "Employee_homePhone")
				.withColumnRenamed("lastname", "Employee_lastname")
				.withColumnRenamed("photo", "Employee_photo")
				.withColumnRenamed("postalCode", "Employee_postalCode")
				.withColumnRenamed("region", "Employee_region")
				.withColumnRenamed("salary", "Employee_salary")
				.withColumnRenamed("title", "Employee_title")
				.withColumnRenamed("notes", "Employee_notes")
				.withColumnRenamed("photoPath", "Employee_photoPath")
				.withColumnRenamed("titleOfCourtesy", "Employee_titleOfCourtesy")
				.withColumnRenamed("logEvents", "Employee_logEvents"),
				orderTDOemployeeReforder.col("mongoSchema_Orders_employeeRef_EmployeeRef").equalTo(employeeTDOemployeeRefemployee.col("mongoSchema_Orders_employeeRef_EmployeeID")));
		Dataset<Order> res_Order_employeeRef = res_employeeRef.select( "id", "freight", "orderDate", "requiredDate", "shipAddress", "shipCity", "shipCountry", "shipName", "shipPostalCode", "shipRegion", "shippedDate", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_employeeRef = res_Order_employeeRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_employeeRef);
		
		
		Dataset<Handle> res_handle_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInComposed_of(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.ProductAttribute> product_condition, conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean product_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		
		//join between 2 SQL tables and a non-relational structure
		// (A) (AB - B)
		product_refilter = new MutableBoolean(false);
		MutableBoolean composed_of_refilter = new MutableBoolean(false);
		Dataset<Composed_ofTDO> res_composed_of_productR_orderR = composed_ofService.getComposed_ofTDOListInProductsInfoAndOrder_DetailsFromrelData(product_condition, composed_of_condition, product_refilter, composed_of_refilter);
		Dataset<OrderTDO> res_orderR_productR = composed_ofService.getOrderTDOListOrderInOrderRInOrdersFromMongoSchema(order_condition, order_refilter);
		if(product_refilter.booleanValue()) {
			if(all == null)
					all = new ProductServiceImpl().getProductList(product_condition);
			joinCondition = null;
				joinCondition = res_composed_of_productR_orderR.col("product.id").equalTo(all.col("id"));
				res_composed_of_productR_orderR = res_composed_of_productR_orderR.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(Composed_ofTDO.class));
		} 
		Dataset<Row> res_row_productR_orderR = res_composed_of_productR_orderR.join(res_orderR_productR.withColumnRenamed("logEvents", "composed_of_logEvents"),
			res_composed_of_productR_orderR.col("relSchema_Order_Details_orderR_OrderRef").equalTo(res_orderR_productR.col("relSchema_Order_Details_orderR_OrderID")));
		Dataset<Order> res_Order_orderR = res_row_productR_orderR.as(Encoders.bean(Order.class));
		datasetsPOJO.add(res_Order_orderR.dropDuplicates(new String[] {"id"}));	
		
		
		
		Dataset<Composed_of> res_composed_of_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertOrder(
		Order order,
		Customer	clientMake_by,
		Shipper	shipperShip_via,
		Employee	employeeHandle){
			boolean inserted = false;
			// Insert in standalone structures
			// Insert in structures containing double embedded role
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			inserted = insertOrderInOrdersFromMyMongoDB(order,clientMake_by,shipperShip_via,employeeHandle)|| inserted ;
			// Insert in ref structures mapped to opposite role of mandatory role  
			return inserted;
		}
	
	public boolean insertOrderInOrdersFromMyMongoDB(Order order,
		Customer	clientMake_by,
		Shipper	shipperShip_via,
		Employee	employeeHandle)	{
			 // Implement Insert in structures with mandatory references
		// In insertRefStruct in MongoDB
		Bson filter = new Document();
		Bson updateOp;
		List<Document> docsList = new ArrayList();
		Document doc = new Document();
		String roleEntityField;
		java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
		
		doc.append("OrderID", order.getId());
		doc.append("Freight", order.getFreight());
		doc.append("OrderDate", order.getOrderDate());
		doc.append("RequiredDate", order.getRequiredDate());
		doc.append("ShipAddress", order.getShipAddress());
		doc.append("ShipCity", order.getShipCity());
		doc.append("ShipCountry", order.getShipCountry());
		doc.append("ShipName", order.getShipName());
		doc.append("ShipPostalCode", order.getShipPostalCode());
		doc.append("ShipRegion", order.getShipRegion());
		doc.append("ShippedDate", order.getShippedDate());
		for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
			String longField = entry.getKey();
			java.util.Map<String, String> values = entry.getValue();
		}	
		
		// Ref 'customerRef' mapped to role 'order'
		doc.append("CustomerID",clientMake_by.getId());
		docsList.add(doc);
		// Ref 'shipperRef' mapped to role 'order'
		doc.append("ShipVia",shipperShip_via.getId());
		docsList.add(doc);
		// Ref 'employeeRef' mapped to role 'order'
		doc.append("EmployeeRef",employeeHandle.getId());
		docsList.add(doc);
			DBConnectionMgr.insertMany(docsList, "Orders", "myMongoDB");
			return true;
		
		}
	private boolean inUpdateMethod = false;
	private List<Row> allOrderIdList = null;
	public void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void updateOrderListInMake_by(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInMake_by(order_condition, null, set);
	}
	public void updateOrderListInMake_byByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInMake_by(null, client_condition, set);
	}
	
	public void updateOrderListInMake_byByClient(
		pojo.Customer client,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInShip_via(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInShip_viaByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInShip_via(shipper_condition, null, set);
	}
	
	public void updateOrderListInShip_viaByShipper(
		pojo.Shipper shipper,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInShip_viaByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInShip_via(null, order_condition, set);
	}
	public void updateOrderListInHandle(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInHandleByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInHandle(employee_condition, null, set);
	}
	
	public void updateOrderListInHandleByEmployee(
		pojo.Employee employee,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInHandleByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInHandle(null, order_condition, set);
	}
	public void updateOrderListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInComposed_ofByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInComposed_of(order_condition, null, null, set);
	}
	public void updateOrderListInComposed_ofByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInComposed_of(null, product_condition, null, set);
	}
	
	public void updateOrderListInComposed_ofByProduct(
		pojo.Product product,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInComposed_of(null, null, composed_of_condition, set);
	}
	
	
	public void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		//TODO
	}
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void deleteOrderListInMake_by(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
		}
	
	public void deleteOrderListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInMake_by(order_condition, null);
	}
	public void deleteOrderListInMake_byByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteOrderListInMake_by(null, client_condition);
	}
	
	public void deleteOrderListInMake_byByClient(
		pojo.Customer client 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInShip_via(	
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteOrderListInShip_viaByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition
	){
		deleteOrderListInShip_via(shipper_condition, null);
	}
	
	public void deleteOrderListInShip_viaByShipper(
		pojo.Shipper shipper 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInShip_viaByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInShip_via(null, order_condition);
	}
	public void deleteOrderListInHandle(	
		conditions.Condition<conditions.EmployeeAttribute> employee_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteOrderListInHandleByEmployeeCondition(
		conditions.Condition<conditions.EmployeeAttribute> employee_condition
	){
		deleteOrderListInHandle(employee_condition, null);
	}
	
	public void deleteOrderListInHandleByEmployee(
		pojo.Employee employee 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInHandleByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInHandle(null, order_condition);
	}
	public void deleteOrderListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of){
			//TODO
		}
	
	public void deleteOrderListInComposed_ofByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInComposed_of(order_condition, null, null);
	}
	public void deleteOrderListInComposed_ofByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteOrderListInComposed_of(null, product_condition, null);
	}
	
	public void deleteOrderListInComposed_ofByProduct(
		pojo.Product product 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		deleteOrderListInComposed_of(null, null, composed_of_condition);
	}
	
}
