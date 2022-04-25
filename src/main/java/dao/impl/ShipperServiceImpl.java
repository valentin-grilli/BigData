package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Shipper;
import conditions.*;
import dao.services.ShipperService;
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


public class ShipperServiceImpl extends ShipperService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShipperServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInShippersFromRelData(Condition<ShipperAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInShippersFromRelDataWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInShippersFromRelData(conditions.SetClause<ShipperAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ShipperAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ShipperAttribute, Object> e : clause.entrySet()) {
				ShipperAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ShipperAttribute.id ) {
					res.add("ShipperID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ShipperAttribute.companyName ) {
					res.add("CompanyName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ShipperAttribute.phone ) {
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
	
	public static Pair<String, List<String>> getSQLWhereClauseInShippersFromRelDataWithTableAlias(Condition<ShipperAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ShipperAttribute attr = ((SimpleCondition<ShipperAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ShipperAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ShipperAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ShipperAttribute.id ) {
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
					if(attr == ShipperAttribute.companyName ) {
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
					if(attr == ShipperAttribute.phone ) {
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
					if(attr == ShipperAttribute.id ) {
						if(op == Operator.EQUALS)
							where =  "ShipperID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ShipperID IS NOT NULL";
					}
					if(attr == ShipperAttribute.companyName ) {
						if(op == Operator.EQUALS)
							where =  "CompanyName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "CompanyName IS NOT NULL";
					}
					if(attr == ShipperAttribute.phone ) {
						if(op == Operator.EQUALS)
							where =  "Phone IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Phone IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInShippersFromRelData(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInShippersFromRelData(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInShippersFromRelData(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInShippersFromRelData(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	public Dataset<Shipper> getShipperListInShippersFromRelData(conditions.Condition<conditions.ShipperAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ShipperServiceImpl.getSQLWhereClauseInShippersFromRelData(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("relData", "Shippers", where);
		
	
		Dataset<Shipper> res = d.map((MapFunction<Row, Shipper>) r -> {
					Shipper shipper_res = new Shipper();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Shipper.Id]
					Integer id = Util.getIntegerValue(r.getAs("ShipperID"));
					shipper_res.setId(id);
					
					// attribute [Shipper.CompanyName]
					String companyName = Util.getStringValue(r.getAs("CompanyName"));
					shipper_res.setCompanyName(companyName);
					
					// attribute [Shipper.Phone]
					String phone = Util.getStringValue(r.getAs("Phone"));
					shipper_res.setPhone(phone);
	
	
	
					return shipper_res;
				}, Encoders.bean(Shipper.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Shipper> getShipperListInShip_via(conditions.Condition<conditions.ShipperAttribute> shipper_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean shipper_refilter = new MutableBoolean(false);
		List<Dataset<Shipper>> datasetsPOJO = new ArrayList<Dataset<Shipper>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'shipperRef'  B->A Scenario
		Dataset<OrderTDO> orderTDOshipperReforder = ship_viaService.getOrderTDOListOrderInShipperRefInOrdersFromMongoSchema(order_condition, order_refilter);
		Dataset<ShipperTDO> shipperTDOshipperRefshipper = ship_viaService.getShipperTDOListShipperInShipperRefInOrdersFromMongoSchema(shipper_condition, shipper_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(order_condition);
			joinCondition = null;
			joinCondition = orderTDOshipperReforder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				orderTDOshipperReforder = orderTDOshipperReforder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDOshipperReforder = orderTDOshipperReforder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_shipperRef = 
			shipperTDOshipperRefshipper.join(orderTDOshipperReforder
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
				.withColumnRenamed("logEvents", "Order_logEvents"),
				shipperTDOshipperRefshipper.col("mongoSchema_Orders_shipperRef_ShipperID").equalTo(orderTDOshipperReforder.col("mongoSchema_Orders_shipperRef_ShipVia")));
		Dataset<Shipper> res_Shipper_shipperRef = res_shipperRef.select( "id", "companyName", "phone", "logEvents").as(Encoders.bean(Shipper.class));
		res_Shipper_shipperRef = res_Shipper_shipperRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Shipper_shipperRef);
		
		Dataset<Ship_via> res_ship_via_shipper;
		Dataset<Shipper> res_Shipper;
		
		
		//Join datasets or return 
		Dataset<Shipper> res = fullOuterJoinsShipper(datasetsPOJO);
		if(res == null)
			return null;
	
		if(shipper_refilter.booleanValue())
			res = res.filter((FilterFunction<Shipper>) r -> shipper_condition == null || shipper_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertShipper(Shipper shipper){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertShipperInShippersFromRelData(shipper) || inserted ;
		return inserted;
	}
	
	public boolean insertShipperInShippersFromRelData(Shipper shipper)	{
		String idvalue="";
		idvalue+=shipper.getId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("ShipperID");
		values.add(shipper.getId());
		columns.add("CompanyName");
		values.add(shipper.getCompanyName());
		columns.add("Phone");
		values.add(shipper.getPhone());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "Shippers", "relData");
			logger.info("Inserted [Shipper] entity ID [{}] in [Shippers] in database [RelData]", idvalue);
		}
		else
			logger.warn("[Shipper] entity ID [{}] already present in [Shippers] in database [RelData]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allShipperIdList = null;
	public void updateShipperList(conditions.Condition<conditions.ShipperAttribute> condition, conditions.SetClause<conditions.ShipperAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInShippersFromRelData = new MutableBoolean(false);
			getSQLWhereClauseInShippersFromRelData(condition, refilterInShippersFromRelData);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInShippersFromRelData.booleanValue())
				updateShipperListInShippersFromRelData(condition, set);
		
	
			if(!refilterInShippersFromRelData.booleanValue())
				updateShipperListInShippersFromRelData(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateShipperListInShippersFromRelData(Condition<ShipperAttribute> condition, SetClause<ShipperAttribute> set) {
		List<String> setClause = ShipperServiceImpl.getSQLSetClauseInShippersFromRelData(set);
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
		Pair<String, List<String>> whereClause = ShipperServiceImpl.getSQLWhereClauseInShippersFromRelData(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE Shippers SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "relData");
		} else {
			if(!inUpdateMethod || allShipperIdList == null)
				allShipperIdList = this.getShipperList(condition).select("id").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allShipperIdList) {
				Condition<ShipperAttribute> conditionId = null;
				conditionId = Condition.simple(ShipperAttribute.id, Operator.EQUALS, row.getAs("id"));
				whereClause = ShipperServiceImpl.getSQLWhereClauseInShippersFromRelData(conditionId, refilter);
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
		
			DBConnectionMgr.updatesInTable(updateQueries, "relData");
		}
		
	}
	
	
	
	public void updateShipper(pojo.Shipper shipper) {
		//TODO using the id
		return;
	}
	public void updateShipperListInShip_via(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.ShipperAttribute> set
	){
		//TODO
	}
	
	public void updateShipperListInShip_viaByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.SetClause<conditions.ShipperAttribute> set
	){
		updateShipperListInShip_via(shipper_condition, null, set);
	}
	public void updateShipperListInShip_viaByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.ShipperAttribute> set
	){
		updateShipperListInShip_via(null, order_condition, set);
	}
	
	public void updateShipperInShip_viaByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.ShipperAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteShipperList(conditions.Condition<conditions.ShipperAttribute> condition){
		//TODO
	}
	
	public void deleteShipper(pojo.Shipper shipper) {
		//TODO using the id
		return;
	}
	public void deleteShipperListInShip_via(	
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteShipperListInShip_viaByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition
	){
		deleteShipperListInShip_via(shipper_condition, null);
	}
	public void deleteShipperListInShip_viaByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteShipperListInShip_via(null, order_condition);
	}
	
	public void deleteShipperInShip_viaByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
