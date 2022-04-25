package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Supplier;
import conditions.*;
import dao.services.SupplierService;
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


public class SupplierServiceImpl extends SupplierService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SupplierServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInSuppliersFromMyMongoDB(conditions.SetClause<SupplierAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<SupplierAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<SupplierAttribute, Object> e : clause.entrySet()) {
				SupplierAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == SupplierAttribute.id ) {
					String fieldName = "SupplierID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.address ) {
					String fieldName = "Address";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.city ) {
					String fieldName = "City";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.companyName ) {
					String fieldName = "CompanyName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.contactName ) {
					String fieldName = "contactName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.contactTitle ) {
					String fieldName = "ContactTitle";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.country ) {
					String fieldName = "Country";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.fax ) {
					String fieldName = "Fax";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.homePage ) {
					String fieldName = "HomePage";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.phone ) {
					String fieldName = "Phone";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.postalCode ) {
					String fieldName = "PostalCode";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == SupplierAttribute.region ) {
					String fieldName = "Region";
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
	
	public static String getBSONMatchQueryInSuppliersFromMyMongoDB(Condition<SupplierAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				SupplierAttribute attr = ((SimpleCondition<SupplierAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<SupplierAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<SupplierAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == SupplierAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "SupplierID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.address ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Address': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.city ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "City': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.companyName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "CompanyName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.contactName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "contactName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.contactTitle ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ContactTitle': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.country ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Country': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.fax ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Fax': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.homePage ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "HomePage': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.phone ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Phone': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.postalCode ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "PostalCode': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == SupplierAttribute.region ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Region': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInSuppliersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInSuppliersFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInSuppliersFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInSuppliersFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(Condition<SupplierAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				SupplierAttribute attr = ((SimpleCondition<SupplierAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<SupplierAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<SupplierAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == SupplierAttribute.id ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "SupplierID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.address ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Address': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.city ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "City': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.companyName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "CompanyName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.contactName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "contactName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.contactTitle ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ContactTitle': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.country ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Country': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.fax ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Fax': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.homePage ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "HomePage': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.phone ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Phone': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.postalCode ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "PostalCode': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == SupplierAttribute.region ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Region': {" + mongoOp + ": " + preparedValue + "}";
					
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
	
	
	
	public Dataset<Supplier> getSupplierListInSuppliersFromMyMongoDB(conditions.Condition<conditions.SupplierAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = SupplierServiceImpl.getBSONMatchQueryInSuppliersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Suppliers", bsonQuery);
	
		Dataset<Supplier> res = dataset.flatMap((FlatMapFunction<Row, Supplier>) r -> {
				Set<Supplier> list_res = new HashSet<Supplier>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Supplier supplier1 = new Supplier();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Supplier.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address")==null)
							supplier1.setAddress(null);
						else{
							supplier1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City")==null)
							supplier1.setCity(null);
						else{
							supplier1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.companyName for field CompanyName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName")==null)
							supplier1.setCompanyName(null);
						else{
							supplier1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.contactName for field contactName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("contactName")) {
						if(nestedRow.getAs("contactName")==null)
							supplier1.setContactName(null);
						else{
							supplier1.setContactName(Util.getStringValue(nestedRow.getAs("contactName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.contactTitle for field ContactTitle			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactTitle")) {
						if(nestedRow.getAs("ContactTitle")==null)
							supplier1.setContactTitle(null);
						else{
							supplier1.setContactTitle(Util.getStringValue(nestedRow.getAs("ContactTitle")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country")==null)
							supplier1.setCountry(null);
						else{
							supplier1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.fax for field Fax			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Fax")) {
						if(nestedRow.getAs("Fax")==null)
							supplier1.setFax(null);
						else{
							supplier1.setFax(Util.getStringValue(nestedRow.getAs("Fax")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.homePage for field HomePage			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePage")) {
						if(nestedRow.getAs("HomePage")==null)
							supplier1.setHomePage(null);
						else{
							supplier1.setHomePage(Util.getStringValue(nestedRow.getAs("HomePage")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.phone for field Phone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone")==null)
							supplier1.setPhone(null);
						else{
							supplier1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode")==null)
							supplier1.setPostalCode(null);
						else{
							supplier1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region")==null)
							supplier1.setRegion(null);
						else{
							supplier1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.id for field SupplierID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("SupplierID")) {
						if(nestedRow.getAs("SupplierID")==null)
							supplier1.setId(null);
						else{
							supplier1.setId(Util.getIntegerValue(nestedRow.getAs("SupplierID")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(supplier1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Supplier.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Supplier> getSupplierListInInsert(conditions.Condition<conditions.SupplierAttribute> supplier_condition,conditions.Condition<conditions.ProductAttribute> product_condition)		{
		MutableBoolean supplier_refilter = new MutableBoolean(false);
		List<Dataset<Supplier>> datasetsPOJO = new ArrayList<Dataset<Supplier>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean product_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		product_refilter = new MutableBoolean(false);
		// For role 'product' in reference 'supplierR'  B->A Scenario
		Dataset<ProductTDO> productTDOsupplierRproduct = insertService.getProductTDOListProductInSupplierRInProductsInfoFromRelSchema(product_condition, product_refilter);
		Dataset<SupplierTDO> supplierTDOsupplierRsupplier = insertService.getSupplierTDOListSupplierInSupplierRInProductsInfoFromRelSchema(supplier_condition, supplier_refilter);
		if(product_refilter.booleanValue()) {
			if(all == null)
				all = new ProductServiceImpl().getProductList(product_condition);
			joinCondition = null;
			joinCondition = productTDOsupplierRproduct.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				productTDOsupplierRproduct = productTDOsupplierRproduct.as("A").join(all).select("A.*").as(Encoders.bean(ProductTDO.class));
			else
				productTDOsupplierRproduct = productTDOsupplierRproduct.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductTDO.class));
		}
		Dataset<Row> res_supplierR = 
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
		Dataset<Supplier> res_Supplier_supplierR = res_supplierR.select( "id", "address", "city", "companyName", "contactName", "contactTitle", "country", "fax", "homePage", "phone", "postalCode", "region", "logEvents").as(Encoders.bean(Supplier.class));
		res_Supplier_supplierR = res_Supplier_supplierR.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Supplier_supplierR);
		
		Dataset<Insert> res_insert_supplier;
		Dataset<Supplier> res_Supplier;
		
		
		//Join datasets or return 
		Dataset<Supplier> res = fullOuterJoinsSupplier(datasetsPOJO);
		if(res == null)
			return null;
	
		if(supplier_refilter.booleanValue())
			res = res.filter((FilterFunction<Supplier>) r -> supplier_condition == null || supplier_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertSupplier(Supplier supplier){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertSupplierInSuppliersFromMyMongoDB(supplier) || inserted ;
		return inserted;
	}
	
	public boolean insertSupplierInSuppliersFromMyMongoDB(Supplier supplier)	{
		String idvalue="";
		idvalue+=supplier.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		Bson filter = new Document();
		Bson updateOp;
		Document docSuppliers_1 = new Document();
		docSuppliers_1.append("Address",supplier.getAddress());
		docSuppliers_1.append("City",supplier.getCity());
		docSuppliers_1.append("CompanyName",supplier.getCompanyName());
		docSuppliers_1.append("contactName",supplier.getContactName());
		docSuppliers_1.append("ContactTitle",supplier.getContactTitle());
		docSuppliers_1.append("Country",supplier.getCountry());
		docSuppliers_1.append("Fax",supplier.getFax());
		docSuppliers_1.append("HomePage",supplier.getHomePage());
		docSuppliers_1.append("Phone",supplier.getPhone());
		docSuppliers_1.append("PostalCode",supplier.getPostalCode());
		docSuppliers_1.append("Region",supplier.getRegion());
		docSuppliers_1.append("SupplierID",supplier.getId());
		
		filter = eq("SupplierID",supplier.getId());
		updateOp = setOnInsert(docSuppliers_1);
		DBConnectionMgr.upsertMany(filter, updateOp, "Suppliers", "myMongoDB");
			logger.info("Inserted [Supplier] entity ID [{}] in [Suppliers] in database [MyMongoDB]", idvalue);
		}
		else
			logger.warn("[Supplier] entity ID [{}] already present in [Suppliers] in database [MyMongoDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allSupplierIdList = null;
	public void updateSupplierList(conditions.Condition<conditions.SupplierAttribute> condition, conditions.SetClause<conditions.SupplierAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInSuppliersFromMyMongoDB = new MutableBoolean(false);
			getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(condition, new ArrayList<String>(), new HashSet<String>(), refilterInSuppliersFromMyMongoDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInSuppliersFromMyMongoDB.booleanValue())
				updateSupplierListInSuppliersFromMyMongoDB(condition, set);
		
	
			if(!refilterInSuppliersFromMyMongoDB.booleanValue())
				updateSupplierListInSuppliersFromMyMongoDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateSupplierListInSuppliersFromMyMongoDB(Condition<SupplierAttribute> condition, SetClause<SupplierAttribute> set) {
		Pair<List<String>, List<String>> updates = getBSONUpdateQueryInSuppliersFromMyMongoDB(set);
		List<String> sets = updates.getLeft();
		final List<String> arrayVariableNames = updates.getRight();
		String setBSON = null;
		for(int i = 0; i < sets.size(); i++) {
			if(i == 0)
				setBSON = sets.get(i);
			else
				setBSON += ", " + sets.get(i);
		}
		
		if(setBSON == null)
			return;
		
		Document updateQuery = null;
		setBSON = "{$set: {" + setBSON + "}}";
		updateQuery = Document.parse(setBSON);
		
		MutableBoolean refilter = new MutableBoolean(false);
		Set<String> arrayVariablesUsed = new HashSet<String>();
		Pair<String, List<String>> queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(condition, arrayVariableNames, arrayVariablesUsed, refilter);
		Document query = null;
		String bsonQuery = queryAndArrayFilter.getLeft();
		if(bsonQuery != null) {
			bsonQuery = "{" + bsonQuery + "}";
			query = Document.parse(bsonQuery);	
		}
		
		List<Bson> arrayFilterDocs = new ArrayList<Bson>();
		List<String> arrayFilters = queryAndArrayFilter.getRight();
		for(String arrayFilter : arrayFilters)
			arrayFilterDocs.add(Document.parse( "{" + arrayFilter + "}"));
		
		for(String arrayVariableName : arrayVariableNames)
			if(!arrayVariablesUsed.contains(arrayVariableName)) {
				arrayFilterDocs.add(Document.parse("{" + arrayVariableName + ": {$exists: true}}"));
			}
		
		
		if(!refilter.booleanValue()) {
			if(arrayFilterDocs.size() == 0) {
				DBConnectionMgr.update(query, updateQuery, "Suppliers", "myMongoDB");
			} else {
				DBConnectionMgr.upsertMany(query, updateQuery, arrayFilterDocs, "Suppliers", "myMongoDB");
			}
		
			
		} else {
			if(!inUpdateMethod || allSupplierIdList == null)
				allSupplierIdList = this.getSupplierList(condition).select("id").collectAsList();
			List<com.mongodb.client.model.UpdateManyModel<Document>> updateQueries = new ArrayList<com.mongodb.client.model.UpdateManyModel<Document>>();
			for(Row row : allSupplierIdList) {
				Condition<SupplierAttribute> conditionId = null;
				conditionId = Condition.simple(SupplierAttribute.id, Operator.EQUALS, row.getAs("id"));
		
				arrayVariablesUsed = new HashSet<String>();
				queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInSuppliersFromMyMongoDB(conditionId, arrayVariableNames, arrayVariablesUsed, refilter);
				query = null;
				bsonQuery = queryAndArrayFilter.getLeft();
				if(bsonQuery != null) {
					bsonQuery = "{" + bsonQuery + "}";
					query = Document.parse(bsonQuery);	
				}
				
				arrayFilterDocs = new ArrayList<Bson>();
				arrayFilters = queryAndArrayFilter.getRight();
				for(String arrayFilter : arrayFilters)
					arrayFilterDocs.add(Document.parse( "{" + arrayFilter + "}"));
				
				for(String arrayVariableName : arrayVariableNames)
					if(!arrayVariablesUsed.contains(arrayVariableName)) {
						arrayFilterDocs.add(Document.parse("{" + arrayVariableName + ": {$exists: true}}"));
					}
				if(arrayFilterDocs.size() == 0)
					updateQueries.add(new com.mongodb.client.model.UpdateManyModel<Document>(query, updateQuery));
				else
					updateQueries.add(new com.mongodb.client.model.UpdateManyModel<Document>(query, updateQuery, new com.mongodb.client.model.UpdateOptions().arrayFilters(arrayFilterDocs)));
			}
		
			DBConnectionMgr.bulkUpdatesInMongoDB(updateQueries, "Suppliers", "myMongoDB");
		}
	}
	
	
	
	public void updateSupplier(pojo.Supplier supplier) {
		//TODO using the id
		return;
	}
	public void updateSupplierListInInsert(
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		
		conditions.SetClause<conditions.SupplierAttribute> set
	){
		//TODO
	}
	
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
	
	
	
	public void deleteSupplierList(conditions.Condition<conditions.SupplierAttribute> condition){
		//TODO
	}
	
	public void deleteSupplier(pojo.Supplier supplier) {
		//TODO using the id
		return;
	}
	public void deleteSupplierListInInsert(	
		conditions.Condition<conditions.SupplierAttribute> supplier_condition,	
		conditions.Condition<conditions.ProductAttribute> product_condition){
			//TODO
		}
	
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
