package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Ship_via;
import java.time.LocalDate;
import java.time.LocalDateTime;
import tdo.*;
import pojo.*;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class Ship_viaService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Ship_viaService.class);
	
	
	// Left side 'ShipVia' of reference [shipperRef ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInShipperRefInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'ShipperID' of reference [shipperRef ]
	public abstract Dataset<ShipperTDO> getShipperTDOListShipperInShipperRefInOrdersFromMongoSchema(Condition<ShipperAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Ship_via> fullLeftOuterJoinBetweenShip_viaAndShipper(Dataset<Ship_via> d1, Dataset<Shipper> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("shipper.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Ship_via>) r -> {
				Ship_via res = new Ship_via();
	
				Shipper shipper = new Shipper();
				Object o = r.getAs("shipper");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						shipper.setId(Util.getIntegerValue(r2.getAs("id")));
						shipper.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						shipper.setPhone(Util.getStringValue(r2.getAs("phone")));
					} 
					if(o instanceof Shipper) {
						shipper = (Shipper) o;
					}
				}
	
				res.setShipper(shipper);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (shipper.getId() != null && id != null && !shipper.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.id': " + shipper.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.id': " + shipper.getId() + " and " + id + "." );
				}
				if(id != null)
					shipper.setId(id);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (shipper.getCompanyName() != null && companyName != null && !shipper.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.companyName': " + shipper.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.companyName': " + shipper.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					shipper.setCompanyName(companyName);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (shipper.getPhone() != null && phone != null && !shipper.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.phone': " + shipper.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.phone': " + shipper.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					shipper.setPhone(phone);
	
				o = r.getAs("order");
				Order order = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getIntegerValue(r2.getAs("id")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				return res;
		}, Encoders.bean(Ship_via.class));
	
		
		
	}
	public static Dataset<Ship_via> fullLeftOuterJoinBetweenShip_viaAndOrder(Dataset<Ship_via> d1, Dataset<Order> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("freight", "A_freight")
			.withColumnRenamed("orderDate", "A_orderDate")
			.withColumnRenamed("requiredDate", "A_requiredDate")
			.withColumnRenamed("shipAddress", "A_shipAddress")
			.withColumnRenamed("shipCity", "A_shipCity")
			.withColumnRenamed("shipCountry", "A_shipCountry")
			.withColumnRenamed("shipName", "A_shipName")
			.withColumnRenamed("shipPostalCode", "A_shipPostalCode")
			.withColumnRenamed("shipRegion", "A_shipRegion")
			.withColumnRenamed("shippedDate", "A_shippedDate")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("order.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Ship_via>) r -> {
				Ship_via res = new Ship_via();
	
				Order order = new Order();
				Object o = r.getAs("order");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getIntegerValue(r2.getAs("id")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (order.getId() != null && id != null && !order.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.id': " + order.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.id': " + order.getId() + " and " + id + "." );
				}
				if(id != null)
					order.setId(id);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (order.getFreight() != null && freight != null && !order.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.freight': " + order.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.freight': " + order.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					order.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (order.getOrderDate() != null && orderDate != null && !order.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					order.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (order.getRequiredDate() != null && requiredDate != null && !order.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					order.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (order.getShipAddress() != null && shipAddress != null && !order.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					order.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (order.getShipCity() != null && shipCity != null && !order.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					order.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (order.getShipCountry() != null && shipCountry != null && !order.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					order.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (order.getShipName() != null && shipName != null && !order.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipName': " + order.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipName': " + order.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					order.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (order.getShipPostalCode() != null && shipPostalCode != null && !order.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					order.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (order.getShipRegion() != null && shipRegion != null && !order.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					order.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (order.getShippedDate() != null && shippedDate != null && !order.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Ship_via - different values found for attribute 'Ship_via.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					order.setShippedDate(shippedDate);
	
				o = r.getAs("shipper");
				Shipper shipper = new Shipper();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						shipper.setId(Util.getIntegerValue(r2.getAs("id")));
						shipper.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						shipper.setPhone(Util.getStringValue(r2.getAs("phone")));
					} 
					if(o instanceof Shipper) {
						shipper = (Shipper) o;
					}
				}
	
				res.setShipper(shipper);
	
				return res;
		}, Encoders.bean(Ship_via.class));
	
		
		
	}
	
	public static Dataset<Ship_via> fullOuterJoinsShip_via(List<Dataset<Ship_via>> datasetsPOJO) {
		return fullOuterJoinsShip_via(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Ship_via> fullLeftOuterJoinsShip_via(List<Dataset<Ship_via>> datasetsPOJO) {
		return fullOuterJoinsShip_via(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Ship_via> fullOuterJoinsShip_via(List<Dataset<Ship_via>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("shipper.id");
	
		idFields.add("order.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Ship_via> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("shipper_id_" + i, d.col("shipper.id"))
				.withColumn("order_id_" + i, d.col("order.id"))
				.withColumnRenamed("shipper", "shipper_" + i)
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("shipper_id_0").equalTo(rows.get(1).col("shipper_id_1"));
		joinCond = joinCond.and(rows.get(0).col("order_id_0").equalTo(rows.get(1).col("order_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("shipper_id_" + (i - 1)).equalTo(rows.get(i).col("shipper_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("order_id_" + (i - 1)).equalTo(rows.get(i).col("order_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Ship_via>) r -> {
				Ship_via ship_via_res = new Ship_via();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							ship_via_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							ship_via_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Shipper shipper_res = new Shipper();
					Order order_res = new Order();
					
					// attribute 'Shipper.id'
					Integer firstNotNull_shipper_id = Util.getIntegerValue(r.getAs("shipper_0.id"));
					shipper_res.setId(firstNotNull_shipper_id);
					// attribute 'Shipper.companyName'
					String firstNotNull_shipper_companyName = Util.getStringValue(r.getAs("shipper_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipper_companyName2 = Util.getStringValue(r.getAs("shipper_" + i + ".companyName"));
						if (firstNotNull_shipper_companyName != null && shipper_companyName2 != null && !firstNotNull_shipper_companyName.equals(shipper_companyName2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.companyName': " + firstNotNull_shipper_companyName + " and " + shipper_companyName2 + "." );
							logger.warn("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.companyName': " + firstNotNull_shipper_companyName + " and " + shipper_companyName2 + "." );
						}
						if (firstNotNull_shipper_companyName == null && shipper_companyName2 != null) {
							firstNotNull_shipper_companyName = shipper_companyName2;
						}
					}
					shipper_res.setCompanyName(firstNotNull_shipper_companyName);
					// attribute 'Shipper.phone'
					String firstNotNull_shipper_phone = Util.getStringValue(r.getAs("shipper_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipper_phone2 = Util.getStringValue(r.getAs("shipper_" + i + ".phone"));
						if (firstNotNull_shipper_phone != null && shipper_phone2 != null && !firstNotNull_shipper_phone.equals(shipper_phone2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.phone': " + firstNotNull_shipper_phone + " and " + shipper_phone2 + "." );
							logger.warn("Data consistency problem for [Shipper - id :"+shipper_res.getId()+"]: different values found for attribute 'Shipper.phone': " + firstNotNull_shipper_phone + " and " + shipper_phone2 + "." );
						}
						if (firstNotNull_shipper_phone == null && shipper_phone2 != null) {
							firstNotNull_shipper_phone = shipper_phone2;
						}
					}
					shipper_res.setPhone(firstNotNull_shipper_phone);
					// attribute 'Order.id'
					Integer firstNotNull_order_id = Util.getIntegerValue(r.getAs("order_0.id"));
					order_res.setId(firstNotNull_order_id);
					// attribute 'Order.freight'
					Double firstNotNull_order_freight = Util.getDoubleValue(r.getAs("order_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double order_freight2 = Util.getDoubleValue(r.getAs("order_" + i + ".freight"));
						if (firstNotNull_order_freight != null && order_freight2 != null && !firstNotNull_order_freight.equals(order_freight2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
						}
						if (firstNotNull_order_freight == null && order_freight2 != null) {
							firstNotNull_order_freight = order_freight2;
						}
					}
					order_res.setFreight(firstNotNull_order_freight);
					// attribute 'Order.orderDate'
					LocalDate firstNotNull_order_orderDate = Util.getLocalDateValue(r.getAs("order_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_orderDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".orderDate"));
						if (firstNotNull_order_orderDate != null && order_orderDate2 != null && !firstNotNull_order_orderDate.equals(order_orderDate2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
						}
						if (firstNotNull_order_orderDate == null && order_orderDate2 != null) {
							firstNotNull_order_orderDate = order_orderDate2;
						}
					}
					order_res.setOrderDate(firstNotNull_order_orderDate);
					// attribute 'Order.requiredDate'
					LocalDate firstNotNull_order_requiredDate = Util.getLocalDateValue(r.getAs("order_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_requiredDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".requiredDate"));
						if (firstNotNull_order_requiredDate != null && order_requiredDate2 != null && !firstNotNull_order_requiredDate.equals(order_requiredDate2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
						}
						if (firstNotNull_order_requiredDate == null && order_requiredDate2 != null) {
							firstNotNull_order_requiredDate = order_requiredDate2;
						}
					}
					order_res.setRequiredDate(firstNotNull_order_requiredDate);
					// attribute 'Order.shipAddress'
					String firstNotNull_order_shipAddress = Util.getStringValue(r.getAs("order_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipAddress2 = Util.getStringValue(r.getAs("order_" + i + ".shipAddress"));
						if (firstNotNull_order_shipAddress != null && order_shipAddress2 != null && !firstNotNull_order_shipAddress.equals(order_shipAddress2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
						}
						if (firstNotNull_order_shipAddress == null && order_shipAddress2 != null) {
							firstNotNull_order_shipAddress = order_shipAddress2;
						}
					}
					order_res.setShipAddress(firstNotNull_order_shipAddress);
					// attribute 'Order.shipCity'
					String firstNotNull_order_shipCity = Util.getStringValue(r.getAs("order_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCity2 = Util.getStringValue(r.getAs("order_" + i + ".shipCity"));
						if (firstNotNull_order_shipCity != null && order_shipCity2 != null && !firstNotNull_order_shipCity.equals(order_shipCity2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
						}
						if (firstNotNull_order_shipCity == null && order_shipCity2 != null) {
							firstNotNull_order_shipCity = order_shipCity2;
						}
					}
					order_res.setShipCity(firstNotNull_order_shipCity);
					// attribute 'Order.shipCountry'
					String firstNotNull_order_shipCountry = Util.getStringValue(r.getAs("order_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCountry2 = Util.getStringValue(r.getAs("order_" + i + ".shipCountry"));
						if (firstNotNull_order_shipCountry != null && order_shipCountry2 != null && !firstNotNull_order_shipCountry.equals(order_shipCountry2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
						}
						if (firstNotNull_order_shipCountry == null && order_shipCountry2 != null) {
							firstNotNull_order_shipCountry = order_shipCountry2;
						}
					}
					order_res.setShipCountry(firstNotNull_order_shipCountry);
					// attribute 'Order.shipName'
					String firstNotNull_order_shipName = Util.getStringValue(r.getAs("order_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipName2 = Util.getStringValue(r.getAs("order_" + i + ".shipName"));
						if (firstNotNull_order_shipName != null && order_shipName2 != null && !firstNotNull_order_shipName.equals(order_shipName2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
						}
						if (firstNotNull_order_shipName == null && order_shipName2 != null) {
							firstNotNull_order_shipName = order_shipName2;
						}
					}
					order_res.setShipName(firstNotNull_order_shipName);
					// attribute 'Order.shipPostalCode'
					String firstNotNull_order_shipPostalCode = Util.getStringValue(r.getAs("order_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipPostalCode2 = Util.getStringValue(r.getAs("order_" + i + ".shipPostalCode"));
						if (firstNotNull_order_shipPostalCode != null && order_shipPostalCode2 != null && !firstNotNull_order_shipPostalCode.equals(order_shipPostalCode2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
						}
						if (firstNotNull_order_shipPostalCode == null && order_shipPostalCode2 != null) {
							firstNotNull_order_shipPostalCode = order_shipPostalCode2;
						}
					}
					order_res.setShipPostalCode(firstNotNull_order_shipPostalCode);
					// attribute 'Order.shipRegion'
					String firstNotNull_order_shipRegion = Util.getStringValue(r.getAs("order_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipRegion2 = Util.getStringValue(r.getAs("order_" + i + ".shipRegion"));
						if (firstNotNull_order_shipRegion != null && order_shipRegion2 != null && !firstNotNull_order_shipRegion.equals(order_shipRegion2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
						}
						if (firstNotNull_order_shipRegion == null && order_shipRegion2 != null) {
							firstNotNull_order_shipRegion = order_shipRegion2;
						}
					}
					order_res.setShipRegion(firstNotNull_order_shipRegion);
					// attribute 'Order.shippedDate'
					LocalDate firstNotNull_order_shippedDate = Util.getLocalDateValue(r.getAs("order_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_shippedDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".shippedDate"));
						if (firstNotNull_order_shippedDate != null && order_shippedDate2 != null && !firstNotNull_order_shippedDate.equals(order_shippedDate2)) {
							ship_via_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
						}
						if (firstNotNull_order_shippedDate == null && order_shippedDate2 != null) {
							firstNotNull_order_shippedDate = order_shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_order_shippedDate);
	
					ship_via_res.setShipper(shipper_res);
					ship_via_res.setOrder(order_res);
					return ship_via_res;
		}
		, Encoders.bean(Ship_via.class));
	
	}
	
	//Empty arguments
	public Dataset<Ship_via> getShip_viaList(){
		 return getShip_viaList(null,null);
	}
	
	public abstract Dataset<Ship_via> getShip_viaList(
		Condition<ShipperAttribute> shipper_condition,
		Condition<OrderAttribute> order_condition);
	
	public Dataset<Ship_via> getShip_viaListByShipperCondition(
		Condition<ShipperAttribute> shipper_condition
	){
		return getShip_viaList(shipper_condition, null);
	}
	
	public Dataset<Ship_via> getShip_viaListByShipper(Shipper shipper) {
		Condition<ShipperAttribute> cond = null;
		cond = Condition.simple(ShipperAttribute.id, Operator.EQUALS, shipper.getId());
		Dataset<Ship_via> res = getShip_viaListByShipperCondition(cond);
	return res;
	}
	public Dataset<Ship_via> getShip_viaListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getShip_viaList(null, order_condition);
	}
	
	public Ship_via getShip_viaByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Ship_via> res = getShip_viaListByOrderCondition(cond);
		List<Ship_via> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	
	
	public abstract void deleteShip_viaList(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteShip_viaListByShipperCondition(
		conditions.Condition<conditions.ShipperAttribute> shipper_condition
	){
		deleteShip_viaList(shipper_condition, null);
	}
	
	public void deleteShip_viaListByShipper(pojo.Shipper shipper) {
		// TODO using id for selecting
		return;
	}
	public void deleteShip_viaListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteShip_viaList(null, order_condition);
	}
	
	public void deleteShip_viaByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
