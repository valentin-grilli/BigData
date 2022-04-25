package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Customer;
import conditions.*;
import dao.services.CustomerService;
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


public class CustomerServiceImpl extends CustomerService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInCustomersFromMyMongoDB(conditions.SetClause<CustomerAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<CustomerAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<CustomerAttribute, Object> e : clause.entrySet()) {
				CustomerAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == CustomerAttribute.id ) {
					String fieldName = "ID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.city ) {
					String fieldName = "City";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.companyName ) {
					String fieldName = "CompanyName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.contactName ) {
					String fieldName = "ContactName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.contactTitle ) {
					String fieldName = "ContactTitle";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.country ) {
					String fieldName = "Country";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.fax ) {
					String fieldName = "Fax";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.phone ) {
					String fieldName = "Phone";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.postalCode ) {
					String fieldName = "PostalCode";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.region ) {
					String fieldName = "Region";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CustomerAttribute.address ) {
					String fieldName = "Address";
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
	
	public static String getBSONMatchQueryInCustomersFromMyMongoDB(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				CustomerAttribute attr = ((SimpleCondition<CustomerAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<CustomerAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<CustomerAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == CustomerAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.city ) {
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
					if(attr == CustomerAttribute.companyName ) {
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
					if(attr == CustomerAttribute.contactName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ContactName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.contactTitle ) {
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
					if(attr == CustomerAttribute.country ) {
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
					if(attr == CustomerAttribute.fax ) {
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
					if(attr == CustomerAttribute.phone ) {
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
					if(attr == CustomerAttribute.postalCode ) {
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
					if(attr == CustomerAttribute.region ) {
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
					if(attr == CustomerAttribute.address ) {
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
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInCustomersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInCustomersFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInCustomersFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInCustomersFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(Condition<CustomerAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				CustomerAttribute attr = ((SimpleCondition<CustomerAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<CustomerAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<CustomerAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == CustomerAttribute.id ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == CustomerAttribute.city ) {
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
					if(attr == CustomerAttribute.companyName ) {
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
					if(attr == CustomerAttribute.contactName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ContactName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == CustomerAttribute.contactTitle ) {
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
					if(attr == CustomerAttribute.country ) {
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
					if(attr == CustomerAttribute.fax ) {
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
					if(attr == CustomerAttribute.phone ) {
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
					if(attr == CustomerAttribute.postalCode ) {
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
					if(attr == CustomerAttribute.region ) {
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
					if(attr == CustomerAttribute.address ) {
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
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
					}
					
				}
	
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
	
	
	
	public Dataset<Customer> getCustomerListInCustomersFromMyMongoDB(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = CustomerServiceImpl.getBSONMatchQueryInCustomersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Customers", bsonQuery);
	
		Dataset<Customer> res = dataset.flatMap((FlatMapFunction<Row, Customer>) r -> {
				Set<Customer> list_res = new HashSet<Customer>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Customer customer1 = new Customer();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Customer.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City")==null)
							customer1.setCity(null);
						else{
							customer1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.companyName for field CompanyName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName")==null)
							customer1.setCompanyName(null);
						else{
							customer1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.contactName for field ContactName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName")==null)
							customer1.setContactName(null);
						else{
							customer1.setContactName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.contactTitle for field ContactTitle			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactTitle")) {
						if(nestedRow.getAs("ContactTitle")==null)
							customer1.setContactTitle(null);
						else{
							customer1.setContactTitle(Util.getStringValue(nestedRow.getAs("ContactTitle")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country")==null)
							customer1.setCountry(null);
						else{
							customer1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.fax for field Fax			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Fax")) {
						if(nestedRow.getAs("Fax")==null)
							customer1.setFax(null);
						else{
							customer1.setFax(Util.getStringValue(nestedRow.getAs("Fax")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.id for field ID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ID")) {
						if(nestedRow.getAs("ID")==null)
							customer1.setId(null);
						else{
							customer1.setId(Util.getStringValue(nestedRow.getAs("ID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.phone for field Phone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone")==null)
							customer1.setPhone(null);
						else{
							customer1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode")==null)
							customer1.setPostalCode(null);
						else{
							customer1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region")==null)
							customer1.setRegion(null);
						else{
							customer1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address")==null)
							customer1.setAddress(null);
						else{
							customer1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(customer1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Customer.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Customer> getClientListInMake_by(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition)		{
		MutableBoolean client_refilter = new MutableBoolean(false);
		List<Dataset<Customer>> datasetsPOJO = new ArrayList<Dataset<Customer>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'customerRef'  B->A Scenario
		Dataset<OrderTDO> orderTDOcustomerReforder = make_byService.getOrderTDOListOrderInCustomerRefInOrdersFromMongoSchema(order_condition, order_refilter);
		Dataset<CustomerTDO> customerTDOcustomerRefclient = make_byService.getCustomerTDOListClientInCustomerRefInOrdersFromMongoSchema(client_condition, client_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(order_condition);
			joinCondition = null;
			joinCondition = orderTDOcustomerReforder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				orderTDOcustomerReforder = orderTDOcustomerReforder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDOcustomerReforder = orderTDOcustomerReforder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		// Multi valued reference
		Dataset<Row> res_customerRef = 
			orderTDOcustomerReforder
			.withColumnRenamed("id", "Order_id")
			.withColumnRenamed("freight", "Order_freight")
			.withColumnRenamed("orderDate", "Order_orderDate")
			.withColumnRenamed("requiredDate", "Order_requiredDate")
			.withColumnRenamed("shipAddress", "Order_shipAddress")
			.withColumnRenamed("shipCity", "Order_shipCity")
			.withColumnRenamed("shipCountry", "Order_shipCountry")
			.withColumnRenamed("shipName", "Order_shipName")
			.withColumnRenamed("shipPostalCode", "Order_shipPostalCode")
			.withColumnRenamed("shipRegion", "Order_shipRegion")
			.withColumnRenamed("shippedDate", "Order_shippedDate")
			.withColumnRenamed("logEvents", "Order_logEvents")
			.join(customerTDOcustomerRefclient,
				functions.array_contains(orderTDOcustomerReforder.col("mongoSchema_Orders_customerRef_CustomerID"),customerTDOcustomerRefclient.col("mongoSchema_Orders_customerRef_ID")));
		Dataset<Customer> res_Customer_customerRef = res_customerRef.select( "id", "city", "companyName", "contactName", "contactTitle", "country", "fax", "phone", "postalCode", "region", "address", "logEvents").as(Encoders.bean(Customer.class));
		res_Customer_customerRef = res_Customer_customerRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Customer_customerRef);
		
		Dataset<Make_by> res_make_by_client;
		Dataset<Customer> res_Customer;
		
		
		//Join datasets or return 
		Dataset<Customer> res = fullOuterJoinsCustomer(datasetsPOJO);
		if(res == null)
			return null;
	
		if(client_refilter.booleanValue())
			res = res.filter((FilterFunction<Customer>) r -> client_condition == null || client_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertCustomer(Customer customer){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCustomerInCustomersFromMyMongoDB(customer) || inserted ;
		return inserted;
	}
	
	public boolean insertCustomerInCustomersFromMyMongoDB(Customer customer)	{
		String idvalue="";
		idvalue+=customer.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		Bson filter = new Document();
		Bson updateOp;
		Document docCustomers_1 = new Document();
		docCustomers_1.append("City",customer.getCity());
		docCustomers_1.append("CompanyName",customer.getCompanyName());
		docCustomers_1.append("ContactName",customer.getContactName());
		docCustomers_1.append("ContactTitle",customer.getContactTitle());
		docCustomers_1.append("Country",customer.getCountry());
		docCustomers_1.append("Fax",customer.getFax());
		docCustomers_1.append("ID",customer.getId());
		docCustomers_1.append("Phone",customer.getPhone());
		docCustomers_1.append("PostalCode",customer.getPostalCode());
		docCustomers_1.append("Region",customer.getRegion());
		docCustomers_1.append("Address",customer.getAddress());
		
		filter = eq("ID",customer.getId());
		updateOp = setOnInsert(docCustomers_1);
		DBConnectionMgr.upsertMany(filter, updateOp, "Customers", "myMongoDB");
			logger.info("Inserted [Customer] entity ID [{}] in [Customers] in database [MyMongoDB]", idvalue);
		}
		else
			logger.warn("[Customer] entity ID [{}] already present in [Customers] in database [MyMongoDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allCustomerIdList = null;
	public void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInCustomersFromMyMongoDB = new MutableBoolean(false);
			getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(condition, new ArrayList<String>(), new HashSet<String>(), refilterInCustomersFromMyMongoDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInCustomersFromMyMongoDB.booleanValue())
				updateCustomerListInCustomersFromMyMongoDB(condition, set);
		
	
			if(!refilterInCustomersFromMyMongoDB.booleanValue())
				updateCustomerListInCustomersFromMyMongoDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateCustomerListInCustomersFromMyMongoDB(Condition<CustomerAttribute> condition, SetClause<CustomerAttribute> set) {
		Pair<List<String>, List<String>> updates = getBSONUpdateQueryInCustomersFromMyMongoDB(set);
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
		Pair<String, List<String>> queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(condition, arrayVariableNames, arrayVariablesUsed, refilter);
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
				DBConnectionMgr.update(query, updateQuery, "Customers", "myMongoDB");
			} else {
				DBConnectionMgr.upsertMany(query, updateQuery, arrayFilterDocs, "Customers", "myMongoDB");
			}
		
			
		} else {
			if(!inUpdateMethod || allCustomerIdList == null)
				allCustomerIdList = this.getCustomerList(condition).select("id").collectAsList();
			List<com.mongodb.client.model.UpdateManyModel<Document>> updateQueries = new ArrayList<com.mongodb.client.model.UpdateManyModel<Document>>();
			for(Row row : allCustomerIdList) {
				Condition<CustomerAttribute> conditionId = null;
				conditionId = Condition.simple(CustomerAttribute.id, Operator.EQUALS, row.getAs("id"));
		
				arrayVariablesUsed = new HashSet<String>();
				queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInCustomersFromMyMongoDB(conditionId, arrayVariableNames, arrayVariablesUsed, refilter);
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
		
			DBConnectionMgr.bulkUpdatesInMongoDB(updateQueries, "Customers", "myMongoDB");
		}
	}
	
	
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public void updateClientListInMake_by(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		//TODO
	}
	
	public void updateClientListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInMake_by(order_condition, null, set);
	}
	
	public void updateClientInMake_byByOrder(
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
	
	
	public void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition){
		//TODO
	}
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public void deleteClientListInMake_by(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
		}
	
	public void deleteClientListInMake_byByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteClientListInMake_by(order_condition, null);
	}
	
	public void deleteClientInMake_byByOrder(
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
