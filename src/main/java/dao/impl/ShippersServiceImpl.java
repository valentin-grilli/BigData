package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Shippers;
import conditions.*;
import dao.services.ShippersService;
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


public class ShippersServiceImpl extends ShippersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShippersServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInShippersFromMyRelDB(Condition<ShippersAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInShippersFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInShippersFromMyRelDB(conditions.SetClause<ShippersAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ShippersAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ShippersAttribute, Object> e : clause.entrySet()) {
				ShippersAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ShippersAttribute.shipperID ) {
					res.add("ShipperID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ShippersAttribute.companyName ) {
					res.add("CompanyName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ShippersAttribute.phone ) {
					res.add("Phone = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInShippersFromMyRelDBWithTableAlias(Condition<ShippersAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ShippersAttribute attr = ((SimpleCondition<ShippersAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ShippersAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ShippersAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ShippersAttribute.shipperID ) {
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
						
						where = tableAlias + "ShipperID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ShippersAttribute.companyName ) {
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
						
						where = tableAlias + "CompanyName " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ShippersAttribute.phone ) {
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
						
						where = tableAlias + "Phone " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == ShippersAttribute.shipperID ) {
						if(op == Operator.EQUALS)
							where =  "ShipperID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ShipperID IS NOT NULL";
					}
					if(attr == ShippersAttribute.companyName ) {
						if(op == Operator.EQUALS)
							where =  "CompanyName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "CompanyName IS NOT NULL";
					}
					if(attr == ShippersAttribute.phone ) {
						if(op == Operator.EQUALS)
							where =  "Phone IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Phone IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInShippersFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInShippersFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInShippersFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInShippersFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	public Dataset<Shippers> getShippersListInShippersFromMyRelDB(conditions.Condition<conditions.ShippersAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ShippersServiceImpl.getSQLWhereClauseInShippersFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Shippers", where);
		
	
		Dataset<Shippers> res = d.map((MapFunction<Row, Shippers>) r -> {
					Shippers shippers_res = new Shippers();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Shippers.ShipperID]
					Integer shipperID = Util.getIntegerValue(r.getAs("ShipperID"));
					shippers_res.setShipperID(shipperID);
					
					// attribute [Shippers.CompanyName]
					String companyName = Util.getStringValue(r.getAs("CompanyName"));
					shippers_res.setCompanyName(companyName);
					
					// attribute [Shippers.Phone]
					String phone = Util.getStringValue(r.getAs("Phone"));
					shippers_res.setPhone(phone);
	
	
	
					return shippers_res;
				}, Encoders.bean(Shippers.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Shippers> getShipperListInShips(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,conditions.Condition<conditions.ShippersAttribute> shipper_condition)		{
		MutableBoolean shipper_refilter = new MutableBoolean(false);
		List<Dataset<Shippers>> datasetsPOJO = new ArrayList<Dataset<Shippers>>();
		Dataset<Orders> all = null;
		boolean all_already_persisted = false;
		MutableBoolean shippedOrder_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		shippedOrder_refilter = new MutableBoolean(false);
		// For role 'shippedOrder' in reference 'deliver'  B->A Scenario
		Dataset<OrdersTDO> ordersTDOdelivershippedOrder = shipsService.getOrdersTDOListShippedOrderInDeliverInOrdersFromMongoDB(shippedOrder_condition, shippedOrder_refilter);
		Dataset<ShippersTDO> shippersTDOdelivershipper = shipsService.getShippersTDOListShipperInDeliverInOrdersFromMongoDB(shipper_condition, shipper_refilter);
		if(shippedOrder_refilter.booleanValue()) {
			if(all == null)
				all = new OrdersServiceImpl().getOrdersList(shippedOrder_condition);
			joinCondition = null;
			joinCondition = ordersTDOdelivershippedOrder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				ordersTDOdelivershippedOrder = ordersTDOdelivershippedOrder.as("A").join(all).select("A.*").as(Encoders.bean(OrdersTDO.class));
			else
				ordersTDOdelivershippedOrder = ordersTDOdelivershippedOrder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrdersTDO.class));
		}
		Dataset<Row> res_deliver = 
			shippersTDOdelivershipper.join(ordersTDOdelivershippedOrder
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
				shippersTDOdelivershipper.col("mongoDB_Orders_deliver_ShipperID").equalTo(ordersTDOdelivershippedOrder.col("mongoDB_Orders_deliver_ShipVia")));
		Dataset<Shippers> res_Shippers_deliver = res_deliver.select( "shipperID", "companyName", "phone", "logEvents").as(Encoders.bean(Shippers.class));
		res_Shippers_deliver = res_Shippers_deliver.dropDuplicates(new String[] {"shipperID"});
		datasetsPOJO.add(res_Shippers_deliver);
		
		Dataset<Ships> res_ships_shipper;
		Dataset<Shippers> res_Shippers;
		
		
		//Join datasets or return 
		Dataset<Shippers> res = fullOuterJoinsShippers(datasetsPOJO);
		if(res == null)
			return null;
	
		if(shipper_refilter.booleanValue())
			res = res.filter((FilterFunction<Shippers>) r -> shipper_condition == null || shipper_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertShippers(Shippers shippers){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertShippersInShippersFromMyRelDB(shippers) || inserted ;
		return inserted;
	}
	
	public boolean insertShippersInShippersFromMyRelDB(Shippers shippers)	{
		String idvalue="";
		idvalue+=shippers.getShipperID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("ShipperID");
		values.add(shippers.getShipperID());
		columns.add("CompanyName");
		values.add(shippers.getCompanyName());
		columns.add("Phone");
		values.add(shippers.getPhone());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "Shippers", "myRelDB");
			logger.info("Inserted [Shippers] entity ID [{}] in [Shippers] in database [MyRelDB]", idvalue);
		}
		else
			logger.warn("[Shippers] entity ID [{}] already present in [Shippers] in database [MyRelDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allShippersIdList = null;
	public void updateShippersList(conditions.Condition<conditions.ShippersAttribute> condition, conditions.SetClause<conditions.ShippersAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInShippersFromMyRelDB = new MutableBoolean(false);
			getSQLWhereClauseInShippersFromMyRelDB(condition, refilterInShippersFromMyRelDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInShippersFromMyRelDB.booleanValue())
				updateShippersListInShippersFromMyRelDB(condition, set);
		
	
			if(!refilterInShippersFromMyRelDB.booleanValue())
				updateShippersListInShippersFromMyRelDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateShippersListInShippersFromMyRelDB(Condition<ShippersAttribute> condition, SetClause<ShippersAttribute> set) {
		List<String> setClause = ShippersServiceImpl.getSQLSetClauseInShippersFromMyRelDB(set);
		String setSQL = null;
		for(int i = 0; i < setClause.size(); i++) {
			if(i == 0)
				setSQL = setClause.get(i);
			else
				setSQL += ", " + setClause.get(i);
		}
		
		if(setSQL == null)
			return;
		
		MutableBoolean refilter = new MutableBoolean(false);
		Pair<String, List<String>> whereClause = ShippersServiceImpl.getSQLWhereClauseInShippersFromMyRelDB(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE Shippers SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "myRelDB");
		} else {
			if(!inUpdateMethod || allShippersIdList == null)
				allShippersIdList = this.getShippersList(condition).select("shipperID").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allShippersIdList) {
				Condition<ShippersAttribute> conditionId = null;
				conditionId = Condition.simple(ShippersAttribute.shipperID, Operator.EQUALS, row.getAs("shipperID"));
				whereClause = ShippersServiceImpl.getSQLWhereClauseInShippersFromMyRelDB(conditionId, refilter);
				String sql = "UPDATE Shippers SET " + setSQL;
				String where = whereClause.getKey();
				List<String> preparedValues = whereClause.getValue();
				for(String preparedValue : preparedValues) {
					where = where.replaceFirst("\\?", preparedValue);
				}
				if(where != null)
					sql += " WHERE " + where;
				updateQueries.add(sql);
			}
		
			DBConnectionMgr.updatesInTable(updateQueries, "myRelDB");
		}
		
	}
	
	
	
	public void updateShippers(pojo.Shippers shippers) {
		//TODO using the id
		return;
	}
	public void updateShipperListInShips(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		//TODO
	}
	
	public void updateShipperListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		updateShipperListInShips(shippedOrder_condition, null, set);
	}
	
	public void updateShipperInShipsByShippedOrder(
		pojo.Orders shippedOrder,
		conditions.SetClause<conditions.ShippersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateShipperListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		updateShipperListInShips(null, shipper_condition, set);
	}
	
	
	public void deleteShippersList(conditions.Condition<conditions.ShippersAttribute> condition){
		//TODO
	}
	
	public void deleteShippers(pojo.Shippers shippers) {
		//TODO using the id
		return;
	}
	public void deleteShipperListInShips(	
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,	
		conditions.Condition<conditions.ShippersAttribute> shipper_condition){
			//TODO
		}
	
	public void deleteShipperListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShipperListInShips(shippedOrder_condition, null);
	}
	
	public void deleteShipperInShipsByShippedOrder(
		pojo.Orders shippedOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteShipperListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShipperListInShips(null, shipper_condition);
	}
	
}
