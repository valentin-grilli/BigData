package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Make_by;
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


public abstract class Make_byService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Make_byService.class);
	
	
	// method accessing the embedded object customer mapped to role client
	public abstract Dataset<Make_by> getMake_byListInmongoSchemaOrderscustomer(Condition<CustomerAttribute> client_condition, Condition<OrderAttribute> order_condition, MutableBoolean client_refilter, MutableBoolean order_refilter);
	
	public static Dataset<Make_by> fullLeftOuterJoinBetweenMake_byAndOrder(Dataset<Make_by> d1, Dataset<Order> d2) {
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
		return d2_.map((MapFunction<Row, Make_by>) r -> {
				Make_by res = new Make_by();
	
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
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.id': " + order.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.id': " + order.getId() + " and " + id + "." );
				}
				if(id != null)
					order.setId(id);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (order.getFreight() != null && freight != null && !order.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.freight': " + order.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.freight': " + order.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					order.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (order.getOrderDate() != null && orderDate != null && !order.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					order.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (order.getRequiredDate() != null && requiredDate != null && !order.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					order.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (order.getShipAddress() != null && shipAddress != null && !order.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					order.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (order.getShipCity() != null && shipCity != null && !order.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					order.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (order.getShipCountry() != null && shipCountry != null && !order.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					order.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (order.getShipName() != null && shipName != null && !order.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipName': " + order.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipName': " + order.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					order.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (order.getShipPostalCode() != null && shipPostalCode != null && !order.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					order.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (order.getShipRegion() != null && shipRegion != null && !order.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					order.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (order.getShippedDate() != null && shippedDate != null && !order.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					order.setShippedDate(shippedDate);
	
				o = r.getAs("client");
				Customer client = new Customer();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						client.setId(Util.getStringValue(r2.getAs("id")));
						client.setCity(Util.getStringValue(r2.getAs("city")));
						client.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						client.setContactName(Util.getStringValue(r2.getAs("contactName")));
						client.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						client.setCountry(Util.getStringValue(r2.getAs("country")));
						client.setFax(Util.getStringValue(r2.getAs("fax")));
						client.setPhone(Util.getStringValue(r2.getAs("phone")));
						client.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						client.setRegion(Util.getStringValue(r2.getAs("region")));
						client.setAddress(Util.getStringValue(r2.getAs("address")));
					} 
					if(o instanceof Customer) {
						client = (Customer) o;
					}
				}
	
				res.setClient(client);
	
				return res;
		}, Encoders.bean(Make_by.class));
	
		
		
	}
	public static Dataset<Make_by> fullLeftOuterJoinBetweenMake_byAndClient(Dataset<Make_by> d1, Dataset<Customer> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("contactName", "A_contactName")
			.withColumnRenamed("contactTitle", "A_contactTitle")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("fax", "A_fax")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("client.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Make_by>) r -> {
				Make_by res = new Make_by();
	
				Customer client = new Customer();
				Object o = r.getAs("client");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						client.setId(Util.getStringValue(r2.getAs("id")));
						client.setCity(Util.getStringValue(r2.getAs("city")));
						client.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						client.setContactName(Util.getStringValue(r2.getAs("contactName")));
						client.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						client.setCountry(Util.getStringValue(r2.getAs("country")));
						client.setFax(Util.getStringValue(r2.getAs("fax")));
						client.setPhone(Util.getStringValue(r2.getAs("phone")));
						client.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						client.setRegion(Util.getStringValue(r2.getAs("region")));
						client.setAddress(Util.getStringValue(r2.getAs("address")));
					} 
					if(o instanceof Customer) {
						client = (Customer) o;
					}
				}
	
				res.setClient(client);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (client.getId() != null && id != null && !client.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.id': " + client.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.id': " + client.getId() + " and " + id + "." );
				}
				if(id != null)
					client.setId(id);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (client.getCity() != null && city != null && !client.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.city': " + client.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.city': " + client.getCity() + " and " + city + "." );
				}
				if(city != null)
					client.setCity(city);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (client.getCompanyName() != null && companyName != null && !client.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.companyName': " + client.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.companyName': " + client.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					client.setCompanyName(companyName);
				String contactName = Util.getStringValue(r.getAs("A_contactName"));
				if (client.getContactName() != null && contactName != null && !client.getContactName().equals(contactName)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.contactName': " + client.getContactName() + " and " + contactName + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.contactName': " + client.getContactName() + " and " + contactName + "." );
				}
				if(contactName != null)
					client.setContactName(contactName);
				String contactTitle = Util.getStringValue(r.getAs("A_contactTitle"));
				if (client.getContactTitle() != null && contactTitle != null && !client.getContactTitle().equals(contactTitle)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.contactTitle': " + client.getContactTitle() + " and " + contactTitle + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.contactTitle': " + client.getContactTitle() + " and " + contactTitle + "." );
				}
				if(contactTitle != null)
					client.setContactTitle(contactTitle);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (client.getCountry() != null && country != null && !client.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.country': " + client.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.country': " + client.getCountry() + " and " + country + "." );
				}
				if(country != null)
					client.setCountry(country);
				String fax = Util.getStringValue(r.getAs("A_fax"));
				if (client.getFax() != null && fax != null && !client.getFax().equals(fax)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.fax': " + client.getFax() + " and " + fax + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.fax': " + client.getFax() + " and " + fax + "." );
				}
				if(fax != null)
					client.setFax(fax);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (client.getPhone() != null && phone != null && !client.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.phone': " + client.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.phone': " + client.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					client.setPhone(phone);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (client.getPostalCode() != null && postalCode != null && !client.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.postalCode': " + client.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.postalCode': " + client.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					client.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (client.getRegion() != null && region != null && !client.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.region': " + client.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.region': " + client.getRegion() + " and " + region + "." );
				}
				if(region != null)
					client.setRegion(region);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (client.getAddress() != null && address != null && !client.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Make_by - different values found for attribute 'Make_by.address': " + client.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Make_by - different values found for attribute 'Make_by.address': " + client.getAddress() + " and " + address + "." );
				}
				if(address != null)
					client.setAddress(address);
	
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
		}, Encoders.bean(Make_by.class));
	
		
		
	}
	
	public static Dataset<Make_by> fullOuterJoinsMake_by(List<Dataset<Make_by>> datasetsPOJO) {
		return fullOuterJoinsMake_by(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Make_by> fullLeftOuterJoinsMake_by(List<Dataset<Make_by>> datasetsPOJO) {
		return fullOuterJoinsMake_by(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Make_by> fullOuterJoinsMake_by(List<Dataset<Make_by>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("order.id");
	
		idFields.add("client.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Make_by> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("order_id_" + i, d.col("order.id"))
				.withColumn("client_id_" + i, d.col("client.id"))
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("client", "client_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("order_id_0").equalTo(rows.get(1).col("order_id_1"));
		joinCond = joinCond.and(rows.get(0).col("client_id_0").equalTo(rows.get(1).col("client_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("order_id_" + (i - 1)).equalTo(rows.get(i).col("order_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("client_id_" + (i - 1)).equalTo(rows.get(i).col("client_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Make_by>) r -> {
				Make_by make_by_res = new Make_by();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							make_by_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							make_by_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order order_res = new Order();
					Customer client_res = new Customer();
					
					// attribute 'Order.id'
					Integer firstNotNull_order_id = Util.getIntegerValue(r.getAs("order_0.id"));
					order_res.setId(firstNotNull_order_id);
					// attribute 'Order.freight'
					Double firstNotNull_order_freight = Util.getDoubleValue(r.getAs("order_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double order_freight2 = Util.getDoubleValue(r.getAs("order_" + i + ".freight"));
						if (firstNotNull_order_freight != null && order_freight2 != null && !firstNotNull_order_freight.equals(order_freight2)) {
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
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
							make_by_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
						}
						if (firstNotNull_order_shippedDate == null && order_shippedDate2 != null) {
							firstNotNull_order_shippedDate = order_shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_order_shippedDate);
					// attribute 'Customer.id'
					String firstNotNull_client_id = Util.getStringValue(r.getAs("client_0.id"));
					client_res.setId(firstNotNull_client_id);
					// attribute 'Customer.city'
					String firstNotNull_client_city = Util.getStringValue(r.getAs("client_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_city2 = Util.getStringValue(r.getAs("client_" + i + ".city"));
						if (firstNotNull_client_city != null && client_city2 != null && !firstNotNull_client_city.equals(client_city2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.city': " + firstNotNull_client_city + " and " + client_city2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.city': " + firstNotNull_client_city + " and " + client_city2 + "." );
						}
						if (firstNotNull_client_city == null && client_city2 != null) {
							firstNotNull_client_city = client_city2;
						}
					}
					client_res.setCity(firstNotNull_client_city);
					// attribute 'Customer.companyName'
					String firstNotNull_client_companyName = Util.getStringValue(r.getAs("client_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_companyName2 = Util.getStringValue(r.getAs("client_" + i + ".companyName"));
						if (firstNotNull_client_companyName != null && client_companyName2 != null && !firstNotNull_client_companyName.equals(client_companyName2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.companyName': " + firstNotNull_client_companyName + " and " + client_companyName2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.companyName': " + firstNotNull_client_companyName + " and " + client_companyName2 + "." );
						}
						if (firstNotNull_client_companyName == null && client_companyName2 != null) {
							firstNotNull_client_companyName = client_companyName2;
						}
					}
					client_res.setCompanyName(firstNotNull_client_companyName);
					// attribute 'Customer.contactName'
					String firstNotNull_client_contactName = Util.getStringValue(r.getAs("client_0.contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_contactName2 = Util.getStringValue(r.getAs("client_" + i + ".contactName"));
						if (firstNotNull_client_contactName != null && client_contactName2 != null && !firstNotNull_client_contactName.equals(client_contactName2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.contactName': " + firstNotNull_client_contactName + " and " + client_contactName2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.contactName': " + firstNotNull_client_contactName + " and " + client_contactName2 + "." );
						}
						if (firstNotNull_client_contactName == null && client_contactName2 != null) {
							firstNotNull_client_contactName = client_contactName2;
						}
					}
					client_res.setContactName(firstNotNull_client_contactName);
					// attribute 'Customer.contactTitle'
					String firstNotNull_client_contactTitle = Util.getStringValue(r.getAs("client_0.contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_contactTitle2 = Util.getStringValue(r.getAs("client_" + i + ".contactTitle"));
						if (firstNotNull_client_contactTitle != null && client_contactTitle2 != null && !firstNotNull_client_contactTitle.equals(client_contactTitle2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.contactTitle': " + firstNotNull_client_contactTitle + " and " + client_contactTitle2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.contactTitle': " + firstNotNull_client_contactTitle + " and " + client_contactTitle2 + "." );
						}
						if (firstNotNull_client_contactTitle == null && client_contactTitle2 != null) {
							firstNotNull_client_contactTitle = client_contactTitle2;
						}
					}
					client_res.setContactTitle(firstNotNull_client_contactTitle);
					// attribute 'Customer.country'
					String firstNotNull_client_country = Util.getStringValue(r.getAs("client_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_country2 = Util.getStringValue(r.getAs("client_" + i + ".country"));
						if (firstNotNull_client_country != null && client_country2 != null && !firstNotNull_client_country.equals(client_country2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.country': " + firstNotNull_client_country + " and " + client_country2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.country': " + firstNotNull_client_country + " and " + client_country2 + "." );
						}
						if (firstNotNull_client_country == null && client_country2 != null) {
							firstNotNull_client_country = client_country2;
						}
					}
					client_res.setCountry(firstNotNull_client_country);
					// attribute 'Customer.fax'
					String firstNotNull_client_fax = Util.getStringValue(r.getAs("client_0.fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_fax2 = Util.getStringValue(r.getAs("client_" + i + ".fax"));
						if (firstNotNull_client_fax != null && client_fax2 != null && !firstNotNull_client_fax.equals(client_fax2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.fax': " + firstNotNull_client_fax + " and " + client_fax2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.fax': " + firstNotNull_client_fax + " and " + client_fax2 + "." );
						}
						if (firstNotNull_client_fax == null && client_fax2 != null) {
							firstNotNull_client_fax = client_fax2;
						}
					}
					client_res.setFax(firstNotNull_client_fax);
					// attribute 'Customer.phone'
					String firstNotNull_client_phone = Util.getStringValue(r.getAs("client_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_phone2 = Util.getStringValue(r.getAs("client_" + i + ".phone"));
						if (firstNotNull_client_phone != null && client_phone2 != null && !firstNotNull_client_phone.equals(client_phone2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.phone': " + firstNotNull_client_phone + " and " + client_phone2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.phone': " + firstNotNull_client_phone + " and " + client_phone2 + "." );
						}
						if (firstNotNull_client_phone == null && client_phone2 != null) {
							firstNotNull_client_phone = client_phone2;
						}
					}
					client_res.setPhone(firstNotNull_client_phone);
					// attribute 'Customer.postalCode'
					String firstNotNull_client_postalCode = Util.getStringValue(r.getAs("client_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_postalCode2 = Util.getStringValue(r.getAs("client_" + i + ".postalCode"));
						if (firstNotNull_client_postalCode != null && client_postalCode2 != null && !firstNotNull_client_postalCode.equals(client_postalCode2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.postalCode': " + firstNotNull_client_postalCode + " and " + client_postalCode2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.postalCode': " + firstNotNull_client_postalCode + " and " + client_postalCode2 + "." );
						}
						if (firstNotNull_client_postalCode == null && client_postalCode2 != null) {
							firstNotNull_client_postalCode = client_postalCode2;
						}
					}
					client_res.setPostalCode(firstNotNull_client_postalCode);
					// attribute 'Customer.region'
					String firstNotNull_client_region = Util.getStringValue(r.getAs("client_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_region2 = Util.getStringValue(r.getAs("client_" + i + ".region"));
						if (firstNotNull_client_region != null && client_region2 != null && !firstNotNull_client_region.equals(client_region2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.region': " + firstNotNull_client_region + " and " + client_region2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.region': " + firstNotNull_client_region + " and " + client_region2 + "." );
						}
						if (firstNotNull_client_region == null && client_region2 != null) {
							firstNotNull_client_region = client_region2;
						}
					}
					client_res.setRegion(firstNotNull_client_region);
					// attribute 'Customer.address'
					String firstNotNull_client_address = Util.getStringValue(r.getAs("client_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String client_address2 = Util.getStringValue(r.getAs("client_" + i + ".address"));
						if (firstNotNull_client_address != null && client_address2 != null && !firstNotNull_client_address.equals(client_address2)) {
							make_by_res.addLogEvent("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.address': " + firstNotNull_client_address + " and " + client_address2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+client_res.getId()+"]: different values found for attribute 'Customer.address': " + firstNotNull_client_address + " and " + client_address2 + "." );
						}
						if (firstNotNull_client_address == null && client_address2 != null) {
							firstNotNull_client_address = client_address2;
						}
					}
					client_res.setAddress(firstNotNull_client_address);
	
					make_by_res.setOrder(order_res);
					make_by_res.setClient(client_res);
					return make_by_res;
		}
		, Encoders.bean(Make_by.class));
	
	}
	
	//Empty arguments
	public Dataset<Make_by> getMake_byList(){
		 return getMake_byList(null,null);
	}
	
	public abstract Dataset<Make_by> getMake_byList(
		Condition<OrderAttribute> order_condition,
		Condition<CustomerAttribute> client_condition);
	
	public Dataset<Make_by> getMake_byListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getMake_byList(order_condition, null);
	}
	
	public Dataset<Make_by> getMake_byListByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, order.getId());
		Dataset<Make_by> res = getMake_byListByOrderCondition(cond);
	return res;
	}
	public Dataset<Make_by> getMake_byListByClientCondition(
		Condition<CustomerAttribute> client_condition
	){
		return getMake_byList(null, client_condition);
	}
	
	public Make_by getMake_byByClient(Customer client) {
		Condition<CustomerAttribute> cond = null;
		cond = Condition.simple(CustomerAttribute.id, Operator.EQUALS, client.getId());
		Dataset<Make_by> res = getMake_byListByClientCondition(cond);
		List<Make_by> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	
	
	public abstract void deleteMake_byList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public void deleteMake_byListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteMake_byList(order_condition, null);
	}
	
	public void deleteMake_byListByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteMake_byListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteMake_byList(null, client_condition);
	}
	
	public void deleteMake_byByClient(pojo.Customer client) {
		// TODO using id for selecting
		return;
	}
		
}
