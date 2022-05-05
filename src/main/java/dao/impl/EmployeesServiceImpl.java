package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Employees;
import conditions.*;
import dao.services.EmployeesService;
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


public class EmployeesServiceImpl extends EmployeesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeesServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInEmployeesFromMyMongoDB(conditions.SetClause<EmployeesAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<EmployeesAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<EmployeesAttribute, Object> e : clause.entrySet()) {
				EmployeesAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == EmployeesAttribute.employeeID ) {
					String fieldName = "EmployeeID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.lastName ) {
					String fieldName = "LastName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.firstName ) {
					String fieldName = "FirstName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.title ) {
					String fieldName = "Title";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.titleOfCourtesy ) {
					String fieldName = "TitleOfCourtesy";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.birthDate ) {
					String fieldName = "BirthDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.hireDate ) {
					String fieldName = "HireDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.address ) {
					String fieldName = "Address";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.city ) {
					String fieldName = "City";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.region ) {
					String fieldName = "Region";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.postalCode ) {
					String fieldName = "PostalCode";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.country ) {
					String fieldName = "Country";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.homePhone ) {
					String fieldName = "HomePhone";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.extension ) {
					String fieldName = "Extension";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.photo ) {
					String fieldName = "Photo";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.notes ) {
					String fieldName = "Notes";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.photoPath ) {
					String fieldName = "PhotoPath";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.salary ) {
					String fieldName = "Salary";
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
	
	public static String getBSONMatchQueryInEmployeesFromMyMongoDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				EmployeesAttribute attr = ((SimpleCondition<EmployeesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<EmployeesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<EmployeesAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == EmployeesAttribute.employeeID ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "EmployeeID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.lastName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "LastName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.firstName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "FirstName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.title ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Title': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.titleOfCourtesy ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "TitleOfCourtesy': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.birthDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "BirthDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.hireDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "HireDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.address ) {
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
					if(attr == EmployeesAttribute.city ) {
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
					if(attr == EmployeesAttribute.region ) {
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
					if(attr == EmployeesAttribute.postalCode ) {
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
					if(attr == EmployeesAttribute.country ) {
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
					if(attr == EmployeesAttribute.homePhone ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "HomePhone': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.extension ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Extension': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.photo ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Photo': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.notes ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Notes': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.photoPath ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "PhotoPath': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeesAttribute.salary ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Salary': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInEmployeesFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInEmployeesFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(Condition<EmployeesAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				EmployeesAttribute attr = ((SimpleCondition<EmployeesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<EmployeesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<EmployeesAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == EmployeesAttribute.employeeID ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "EmployeeID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.lastName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "LastName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.firstName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "FirstName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.title ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Title': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.titleOfCourtesy ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "TitleOfCourtesy': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.birthDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "BirthDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.hireDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "HireDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.address ) {
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
					if(attr == EmployeesAttribute.city ) {
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
					if(attr == EmployeesAttribute.region ) {
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
					if(attr == EmployeesAttribute.postalCode ) {
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
					if(attr == EmployeesAttribute.country ) {
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
					if(attr == EmployeesAttribute.homePhone ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "HomePhone': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.extension ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Extension': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.photo ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Photo': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.notes ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Notes': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.photoPath ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "PhotoPath': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeesAttribute.salary ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Salary': {" + mongoOp + ": " + preparedValue + "}";
					
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
	
	
	
	public Dataset<Employees> getEmployeesListInEmployeesFromMyMongoDB(conditions.Condition<conditions.EmployeesAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeesServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<Employees> res = dataset.flatMap((FlatMapFunction<Row, Employees>) r -> {
				Set<Employees> list_res = new HashSet<Employees>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Employees employees1 = new Employees();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employees.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID")==null)
							employees1.setEmployeeID(null);
						else{
							employees1.setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName")==null)
							employees1.setLastName(null);
						else{
							employees1.setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName")==null)
							employees1.setFirstName(null);
						else{
							employees1.setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title")==null)
							employees1.setTitle(null);
						else{
							employees1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy")==null)
							employees1.setTitleOfCourtesy(null);
						else{
							employees1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate")==null)
							employees1.setBirthDate(null);
						else{
							employees1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate")==null)
							employees1.setHireDate(null);
						else{
							employees1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone")==null)
							employees1.setHomePhone(null);
						else{
							employees1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension")==null)
							employees1.setExtension(null);
						else{
							employees1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo")==null)
							employees1.setPhoto(null);
						else{
							employees1.setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes")==null)
							employees1.setNotes(null);
						else{
							employees1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath")==null)
							employees1.setPhotoPath(null);
						else{
							employees1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary")==null)
							employees1.setSalary(null);
						else{
							employees1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address")==null)
							employees1.setAddress(null);
						else{
							employees1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City")==null)
							employees1.setCity(null);
						else{
							employees1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region")==null)
							employees1.setRegion(null);
						else{
							employees1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode")==null)
							employees1.setPostalCode(null);
						else{
							employees1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employees.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country")==null)
							employees1.setCountry(null);
						else{
							employees1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(employees1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Employees.class));
		res= res.dropDuplicates(new String[]{"employeeID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Employees> getEmployedListInWorks(conditions.Condition<conditions.EmployeesAttribute> employed_condition,conditions.Condition<conditions.TerritoriesAttribute> territories_condition)		{
		MutableBoolean employed_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Territories> all = null;
		boolean all_already_persisted = false;
		MutableBoolean territories_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Works> res_works_employed;
		Dataset<Employees> res_Employees;
		// Role 'employed' mapped to EmbeddedObject 'territories' - 'Territories' containing 'Employees'
		territories_refilter = new MutableBoolean(false);
		res_works_employed = worksService.getWorksListInmongoDBEmployeesterritories(employed_condition, territories_condition, employed_refilter, territories_refilter);
		if(territories_refilter.booleanValue()) {
			if(all == null)
				all = new TerritoriesServiceImpl().getTerritoriesList(territories_condition);
			joinCondition = null;
			joinCondition = res_works_employed.col("territories.territoryID").equalTo(all.col("territoryID"));
			if(joinCondition == null)
				res_Employees = res_works_employed.join(all).select("employed.*").as(Encoders.bean(Employees.class));
			else
				res_Employees = res_works_employed.join(all, joinCondition).select("employed.*").as(Encoders.bean(Employees.class));
		
		} else
			res_Employees = res_works_employed.map((MapFunction<Works,Employees>) r -> r.getEmployed(), Encoders.bean(Employees.class));
		res_Employees = res_Employees.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees);
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(employed_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> employed_condition == null || employed_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Employees> getSubordoneeListInReportsTo(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,conditions.Condition<conditions.EmployeesAttribute> boss_condition)		{
		MutableBoolean subordonee_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean boss_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'subordonee' in reference 'manager'. A->B Scenario
		boss_refilter = new MutableBoolean(false);
		Dataset<EmployeesTDO> employeesTDOmanagersubordonee = reportsToService.getEmployeesTDOListSubordoneeInManagerInEmployeesFromMongoDB(subordonee_condition, subordonee_refilter);
		Dataset<EmployeesTDO> employeesTDOmanagerboss = reportsToService.getEmployeesTDOListBossInManagerInEmployeesFromMongoDB(boss_condition, boss_refilter);
		if(boss_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeesServiceImpl().getEmployeesList(boss_condition);
			joinCondition = null;
			joinCondition = employeesTDOmanagerboss.col("employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				employeesTDOmanagerboss = employeesTDOmanagerboss.as("A").join(all).select("A.*").as(Encoders.bean(EmployeesTDO.class));
			else
				employeesTDOmanagerboss = employeesTDOmanagerboss.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(EmployeesTDO.class));
		}
	
		
		Dataset<Row> res_manager = employeesTDOmanagersubordonee.join(employeesTDOmanagerboss
				.withColumnRenamed("employeeID", "Employees_employeeID")
				.withColumnRenamed("lastName", "Employees_lastName")
				.withColumnRenamed("firstName", "Employees_firstName")
				.withColumnRenamed("title", "Employees_title")
				.withColumnRenamed("titleOfCourtesy", "Employees_titleOfCourtesy")
				.withColumnRenamed("birthDate", "Employees_birthDate")
				.withColumnRenamed("hireDate", "Employees_hireDate")
				.withColumnRenamed("address", "Employees_address")
				.withColumnRenamed("city", "Employees_city")
				.withColumnRenamed("region", "Employees_region")
				.withColumnRenamed("postalCode", "Employees_postalCode")
				.withColumnRenamed("country", "Employees_country")
				.withColumnRenamed("homePhone", "Employees_homePhone")
				.withColumnRenamed("extension", "Employees_extension")
				.withColumnRenamed("photo", "Employees_photo")
				.withColumnRenamed("notes", "Employees_notes")
				.withColumnRenamed("photoPath", "Employees_photoPath")
				.withColumnRenamed("salary", "Employees_salary")
				.withColumnRenamed("logEvents", "Employees_logEvents"),
				employeesTDOmanagersubordonee.col("mongoDB_Employees_manager_ReportsTo").equalTo(employeesTDOmanagerboss.col("mongoDB_Employees_manager_EmployeeID")));
		Dataset<Employees> res_Employees_manager = res_manager.select( "employeeID", "lastName", "firstName", "title", "titleOfCourtesy", "birthDate", "hireDate", "address", "city", "region", "postalCode", "country", "homePhone", "extension", "photo", "notes", "photoPath", "salary", "logEvents").as(Encoders.bean(Employees.class));
		
		res_Employees_manager = res_Employees_manager.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees_manager);
		
		
		Dataset<ReportsTo> res_reportsTo_subordonee;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(subordonee_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> subordonee_condition == null || subordonee_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Employees> getBossListInReportsTo(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,conditions.Condition<conditions.EmployeesAttribute> boss_condition)		{
		MutableBoolean boss_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean subordonee_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		subordonee_refilter = new MutableBoolean(false);
		// For role 'subordonee' in reference 'manager'  B->A Scenario
		Dataset<EmployeesTDO> employeesTDOmanagersubordonee = reportsToService.getEmployeesTDOListSubordoneeInManagerInEmployeesFromMongoDB(subordonee_condition, subordonee_refilter);
		Dataset<EmployeesTDO> employeesTDOmanagerboss = reportsToService.getEmployeesTDOListBossInManagerInEmployeesFromMongoDB(boss_condition, boss_refilter);
		if(subordonee_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeesServiceImpl().getEmployeesList(subordonee_condition);
			joinCondition = null;
			joinCondition = employeesTDOmanagersubordonee.col("employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				employeesTDOmanagersubordonee = employeesTDOmanagersubordonee.as("A").join(all).select("A.*").as(Encoders.bean(EmployeesTDO.class));
			else
				employeesTDOmanagersubordonee = employeesTDOmanagersubordonee.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(EmployeesTDO.class));
		}
		Dataset<Row> res_manager = 
			employeesTDOmanagerboss.join(employeesTDOmanagersubordonee
				.withColumnRenamed("employeeID", "Employees_employeeID")
				.withColumnRenamed("lastName", "Employees_lastName")
				.withColumnRenamed("firstName", "Employees_firstName")
				.withColumnRenamed("title", "Employees_title")
				.withColumnRenamed("titleOfCourtesy", "Employees_titleOfCourtesy")
				.withColumnRenamed("birthDate", "Employees_birthDate")
				.withColumnRenamed("hireDate", "Employees_hireDate")
				.withColumnRenamed("address", "Employees_address")
				.withColumnRenamed("city", "Employees_city")
				.withColumnRenamed("region", "Employees_region")
				.withColumnRenamed("postalCode", "Employees_postalCode")
				.withColumnRenamed("country", "Employees_country")
				.withColumnRenamed("homePhone", "Employees_homePhone")
				.withColumnRenamed("extension", "Employees_extension")
				.withColumnRenamed("photo", "Employees_photo")
				.withColumnRenamed("notes", "Employees_notes")
				.withColumnRenamed("photoPath", "Employees_photoPath")
				.withColumnRenamed("salary", "Employees_salary")
				.withColumnRenamed("logEvents", "Employees_logEvents"),
				employeesTDOmanagerboss.col("mongoDB_Employees_manager_EmployeeID").equalTo(employeesTDOmanagersubordonee.col("mongoDB_Employees_manager_ReportsTo")));
		Dataset<Employees> res_Employees_manager = res_manager.select( "employeeID", "lastName", "firstName", "title", "titleOfCourtesy", "birthDate", "hireDate", "address", "city", "region", "postalCode", "country", "homePhone", "extension", "photo", "notes", "photoPath", "salary", "logEvents").as(Encoders.bean(Employees.class));
		res_Employees_manager = res_Employees_manager.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees_manager);
		
		Dataset<ReportsTo> res_reportsTo_boss;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(boss_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> boss_condition == null || boss_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Employees> getEmployeeInChargeListInRegister(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition)		{
		MutableBoolean employeeInCharge_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Orders> all = null;
		boolean all_already_persisted = false;
		MutableBoolean processedOrder_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		processedOrder_refilter = new MutableBoolean(false);
		// For role 'processedOrder' in reference 'encoded'  B->A Scenario
		Dataset<OrdersTDO> ordersTDOencodedprocessedOrder = registerService.getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(processedOrder_condition, processedOrder_refilter);
		Dataset<EmployeesTDO> employeesTDOencodedemployeeInCharge = registerService.getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(employeeInCharge_condition, employeeInCharge_refilter);
		if(processedOrder_refilter.booleanValue()) {
			if(all == null)
				all = new OrdersServiceImpl().getOrdersList(processedOrder_condition);
			joinCondition = null;
			joinCondition = ordersTDOencodedprocessedOrder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				ordersTDOencodedprocessedOrder = ordersTDOencodedprocessedOrder.as("A").join(all).select("A.*").as(Encoders.bean(OrdersTDO.class));
			else
				ordersTDOencodedprocessedOrder = ordersTDOencodedprocessedOrder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrdersTDO.class));
		}
		Dataset<Row> res_encoded = 
			employeesTDOencodedemployeeInCharge.join(ordersTDOencodedprocessedOrder
				.withColumnRenamed("id", "Orders_id")
				.withColumnRenamed("orderDate", "Orders_orderDate")
				.withColumnRenamed("requiredDate", "Orders_requiredDate")
				.withColumnRenamed("shippedDate", "Orders_shippedDate")
				.withColumnRenamed("freight", "Orders_freight")
				.withColumnRenamed("shipName", "Orders_shipName")
				.withColumnRenamed("shipAddress", "Orders_shipAddress")
				.withColumnRenamed("shipCity", "Orders_shipCity")
				.withColumnRenamed("shipRegion", "Orders_shipRegion")
				.withColumnRenamed("shipPostalCode", "Orders_shipPostalCode")
				.withColumnRenamed("shipCountry", "Orders_shipCountry")
				.withColumnRenamed("logEvents", "Orders_logEvents"),
				employeesTDOencodedemployeeInCharge.col("mongoDB_Orders_encoded_EmployeeID").equalTo(ordersTDOencodedprocessedOrder.col("mongoDB_Orders_encoded_EmployeeRef")));
		Dataset<Employees> res_Employees_encoded = res_encoded.select( "employeeID", "lastName", "firstName", "title", "titleOfCourtesy", "birthDate", "hireDate", "address", "city", "region", "postalCode", "country", "homePhone", "extension", "photo", "notes", "photoPath", "salary", "logEvents").as(Encoders.bean(Employees.class));
		res_Employees_encoded = res_Employees_encoded.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees_encoded);
		
		Dataset<Register> res_register_employeeInCharge;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(employeeInCharge_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> employeeInCharge_condition == null || employeeInCharge_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertEmployees(Employees employees){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertEmployeesInEmployeesFromMyMongoDB(employees) || inserted ;
		return inserted;
	}
	
	public boolean insertEmployeesInEmployeesFromMyMongoDB(Employees employees)	{
		String idvalue="";
		idvalue+=employees.getEmployeeID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		Bson filter = new Document();
		Bson updateOp;
		Document docEmployees_1 = new Document();
		docEmployees_1.append("EmployeeID",employees.getEmployeeID());
		docEmployees_1.append("LastName",employees.getLastName());
		docEmployees_1.append("FirstName",employees.getFirstName());
		docEmployees_1.append("Title",employees.getTitle());
		docEmployees_1.append("TitleOfCourtesy",employees.getTitleOfCourtesy());
		docEmployees_1.append("BirthDate",employees.getBirthDate());
		docEmployees_1.append("HireDate",employees.getHireDate());
		docEmployees_1.append("HomePhone",employees.getHomePhone());
		docEmployees_1.append("Extension",employees.getExtension());
		docEmployees_1.append("Photo",employees.getPhoto());
		docEmployees_1.append("Notes",employees.getNotes());
		docEmployees_1.append("PhotoPath",employees.getPhotoPath());
		docEmployees_1.append("Salary",employees.getSalary());
		docEmployees_1.append("Address",employees.getAddress());
		docEmployees_1.append("City",employees.getCity());
		docEmployees_1.append("Region",employees.getRegion());
		docEmployees_1.append("PostalCode",employees.getPostalCode());
		docEmployees_1.append("Country",employees.getCountry());
		
		filter = eq("EmployeeID",employees.getEmployeeID());
		updateOp = setOnInsert(docEmployees_1);
		DBConnectionMgr.upsertMany(filter, updateOp, "Employees", "myMongoDB");
			logger.info("Inserted [Employees] entity ID [{}] in [Employees] in database [MyMongoDB]", idvalue);
		}
		else
			logger.warn("[Employees] entity ID [{}] already present in [Employees] in database [MyMongoDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allEmployeesIdList = null;
	public void updateEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition, conditions.SetClause<conditions.EmployeesAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInEmployeesFromMyMongoDB = new MutableBoolean(false);
			getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(condition, new ArrayList<String>(), new HashSet<String>(), refilterInEmployeesFromMyMongoDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInEmployeesFromMyMongoDB.booleanValue())
				updateEmployeesListInEmployeesFromMyMongoDB(condition, set);
		
	
			if(!refilterInEmployeesFromMyMongoDB.booleanValue())
				updateEmployeesListInEmployeesFromMyMongoDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateEmployeesListInEmployeesFromMyMongoDB(Condition<EmployeesAttribute> condition, SetClause<EmployeesAttribute> set) {
		Pair<List<String>, List<String>> updates = getBSONUpdateQueryInEmployeesFromMyMongoDB(set);
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
		Pair<String, List<String>> queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(condition, arrayVariableNames, arrayVariablesUsed, refilter);
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
				DBConnectionMgr.update(query, updateQuery, "Employees", "myMongoDB");
			} else {
				DBConnectionMgr.upsertMany(query, updateQuery, arrayFilterDocs, "Employees", "myMongoDB");
			}
		
			
		} else {
			if(!inUpdateMethod || allEmployeesIdList == null)
				allEmployeesIdList = this.getEmployeesList(condition).select("employeeID").collectAsList();
			List<com.mongodb.client.model.UpdateManyModel<Document>> updateQueries = new ArrayList<com.mongodb.client.model.UpdateManyModel<Document>>();
			for(Row row : allEmployeesIdList) {
				Condition<EmployeesAttribute> conditionId = null;
				conditionId = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, row.getAs("employeeID"));
		
				arrayVariablesUsed = new HashSet<String>();
				queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(conditionId, arrayVariableNames, arrayVariablesUsed, refilter);
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
		
			DBConnectionMgr.bulkUpdatesInMongoDB(updateQueries, "Employees", "myMongoDB");
		}
	}
	
	
	
	public void updateEmployees(pojo.Employees employees) {
		//TODO using the id
		return;
	}
	public void updateEmployedListInWorks(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateEmployedListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployedListInWorks(employed_condition, null, set);
	}
	public void updateEmployedListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployedListInWorks(null, territories_condition, set);
	}
	
	public void updateEmployedListInWorksByTerritories(
		pojo.Territories territories,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateSubordoneeListInReportsTo(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateSubordoneeListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateSubordoneeListInReportsTo(subordonee_condition, null, set);
	}
	public void updateSubordoneeListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateSubordoneeListInReportsTo(null, boss_condition, set);
	}
	
	public void updateSubordoneeListInReportsToByBoss(
		pojo.Employees boss,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateBossListInReportsTo(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateBossListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateBossListInReportsTo(subordonee_condition, null, set);
	}
	
	public void updateBossInReportsToBySubordonee(
		pojo.Employees subordonee,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateBossListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateBossListInReportsTo(null, boss_condition, set);
	}
	public void updateEmployeeInChargeListInRegister(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateEmployeeInChargeListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployeeInChargeListInRegister(processedOrder_condition, null, set);
	}
	
	public void updateEmployeeInChargeInRegisterByProcessedOrder(
		pojo.Orders processedOrder,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateEmployeeInChargeListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployeeInChargeListInRegister(null, employeeInCharge_condition, set);
	}
	
	
	public void deleteEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition){
		//TODO
	}
	
	public void deleteEmployees(pojo.Employees employees) {
		//TODO using the id
		return;
	}
	public void deleteEmployedListInWorks(	
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
			//TODO
		}
	
	public void deleteEmployedListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteEmployedListInWorks(employed_condition, null);
	}
	public void deleteEmployedListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteEmployedListInWorks(null, territories_condition);
	}
	
	public void deleteEmployedListInWorksByTerritories(
		pojo.Territories territories 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteSubordoneeListInReportsTo(	
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,	
		conditions.Condition<conditions.EmployeesAttribute> boss_condition){
			//TODO
		}
	
	public void deleteSubordoneeListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteSubordoneeListInReportsTo(subordonee_condition, null);
	}
	public void deleteSubordoneeListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteSubordoneeListInReportsTo(null, boss_condition);
	}
	
	public void deleteSubordoneeListInReportsToByBoss(
		pojo.Employees boss 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteBossListInReportsTo(	
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,	
		conditions.Condition<conditions.EmployeesAttribute> boss_condition){
			//TODO
		}
	
	public void deleteBossListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteBossListInReportsTo(subordonee_condition, null);
	}
	
	public void deleteBossInReportsToBySubordonee(
		pojo.Employees subordonee 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteBossListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteBossListInReportsTo(null, boss_condition);
	}
	public void deleteEmployeeInChargeListInRegister(	
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,	
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
			//TODO
		}
	
	public void deleteEmployeeInChargeListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteEmployeeInChargeListInRegister(processedOrder_condition, null);
	}
	
	public void deleteEmployeeInChargeInRegisterByProcessedOrder(
		pojo.Orders processedOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteEmployeeInChargeListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteEmployeeInChargeListInRegister(null, employeeInCharge_condition);
	}
	
}
