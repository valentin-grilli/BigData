package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Composed_of;
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


public abstract class Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofService.class);
	// A<-AB->B . getAListInREL
	//join structure
	// Left side 'OrderRef' of reference [orderR ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInOrderRInOrdersFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	
	// A<-AB->B . getBListInREL
	
	
	public abstract Dataset<Composed_ofTDO> getComposed_ofTDOListInProductsInfoAndOrder_DetailsFromrelData(Condition<ProductAttribute> product_cond, Condition<Composed_ofAttribute> composed_of_cond, MutableBoolean refilterFlag, MutableBoolean composed_of_refilter);
	
	
	
	
	
	
	public static Dataset<Composed_of> fullLeftOuterJoinBetweenComposed_ofAndOrder(Dataset<Composed_of> d1, Dataset<Order> d2) {
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
		return d2_.map((MapFunction<Row, Composed_of>) r -> {
				Composed_of res = new Composed_of();
				res.setUnitPrice(r.getAs("unitPrice"));
				res.setQuantity(r.getAs("quantity"));
				res.setDiscount(r.getAs("discount"));
	
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
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + order.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + order.getId() + " and " + id + "." );
				}
				if(id != null)
					order.setId(id);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (order.getFreight() != null && freight != null && !order.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.freight': " + order.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.freight': " + order.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					order.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (order.getOrderDate() != null && orderDate != null && !order.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					order.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (order.getRequiredDate() != null && requiredDate != null && !order.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					order.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (order.getShipAddress() != null && shipAddress != null && !order.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					order.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (order.getShipCity() != null && shipCity != null && !order.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					order.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (order.getShipCountry() != null && shipCountry != null && !order.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					order.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (order.getShipName() != null && shipName != null && !order.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipName': " + order.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipName': " + order.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					order.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (order.getShipPostalCode() != null && shipPostalCode != null && !order.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					order.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (order.getShipRegion() != null && shipRegion != null && !order.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					order.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (order.getShippedDate() != null && shippedDate != null && !order.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					order.setShippedDate(shippedDate);
	
				o = r.getAs("product");
				Product product = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setId(Util.getIntegerValue(r2.getAs("id")));
						product.setName(Util.getStringValue(r2.getAs("name")));
						product.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
						product.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof Product) {
						product = (Product) o;
					}
				}
	
				res.setProduct(product);
	
				return res;
		}, Encoders.bean(Composed_of.class));
	
		
		
	}
	public static Dataset<Composed_of> fullLeftOuterJoinBetweenComposed_ofAndProduct(Dataset<Composed_of> d1, Dataset<Product> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("name", "A_name")
			.withColumnRenamed("supplierRef", "A_supplierRef")
			.withColumnRenamed("categoryRef", "A_categoryRef")
			.withColumnRenamed("quantityPerUnit", "A_quantityPerUnit")
			.withColumnRenamed("unitPrice", "A_unitPrice")
			.withColumnRenamed("reorderLevel", "A_reorderLevel")
			.withColumnRenamed("discontinued", "A_discontinued")
			.withColumnRenamed("unitsInStock", "A_unitsInStock")
			.withColumnRenamed("unitsOnOrder", "A_unitsOnOrder")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("product.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Composed_of>) r -> {
				Composed_of res = new Composed_of();
				res.setUnitPrice(r.getAs("unitPrice"));
				res.setQuantity(r.getAs("quantity"));
				res.setDiscount(r.getAs("discount"));
	
				Product product = new Product();
				Object o = r.getAs("product");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setId(Util.getIntegerValue(r2.getAs("id")));
						product.setName(Util.getStringValue(r2.getAs("name")));
						product.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
						product.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof Product) {
						product = (Product) o;
					}
				}
	
				res.setProduct(product);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (product.getId() != null && id != null && !product.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + product.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + product.getId() + " and " + id + "." );
				}
				if(id != null)
					product.setId(id);
				String name = Util.getStringValue(r.getAs("A_name"));
				if (product.getName() != null && name != null && !product.getName().equals(name)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.name': " + product.getName() + " and " + name + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.name': " + product.getName() + " and " + name + "." );
				}
				if(name != null)
					product.setName(name);
				Integer supplierRef = Util.getIntegerValue(r.getAs("A_supplierRef"));
				if (product.getSupplierRef() != null && supplierRef != null && !product.getSupplierRef().equals(supplierRef)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
				}
				if(supplierRef != null)
					product.setSupplierRef(supplierRef);
				Integer categoryRef = Util.getIntegerValue(r.getAs("A_categoryRef"));
				if (product.getCategoryRef() != null && categoryRef != null && !product.getCategoryRef().equals(categoryRef)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
				}
				if(categoryRef != null)
					product.setCategoryRef(categoryRef);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (product.getQuantityPerUnit() != null && quantityPerUnit != null && !product.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					product.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (product.getUnitPrice() != null && unitPrice != null && !product.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					product.setUnitPrice(unitPrice);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (product.getReorderLevel() != null && reorderLevel != null && !product.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					product.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (product.getDiscontinued() != null && discontinued != null && !product.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					product.setDiscontinued(discontinued);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (product.getUnitsInStock() != null && unitsInStock != null && !product.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					product.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (product.getUnitsOnOrder() != null && unitsOnOrder != null && !product.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					product.setUnitsOnOrder(unitsOnOrder);
	
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
		}, Encoders.bean(Composed_of.class));
	
		
		
	}
	
	public static Dataset<Composed_of> fullOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO) {
		return fullOuterJoinsComposed_of(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Composed_of> fullLeftOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO) {
		return fullOuterJoinsComposed_of(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Composed_of> fullOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("order.id");
	
		idFields.add("product.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Composed_of> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("order_id_" + i, d.col("order.id"))
				.withColumn("product_id_" + i, d.col("product.id"))
				.withColumnRenamed("unitPrice", "unitPrice_" + i)
				.withColumnRenamed("quantity", "quantity_" + i)
				.withColumnRenamed("discount", "discount_" + i)
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("product", "product_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("order_id_0").equalTo(rows.get(1).col("order_id_1"));
		joinCond = joinCond.and(rows.get(0).col("product_id_0").equalTo(rows.get(1).col("product_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("order_id_" + (i - 1)).equalTo(rows.get(i).col("order_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("product_id_" + (i - 1)).equalTo(rows.get(i).col("product_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Composed_of>) r -> {
				Composed_of composed_of_res = new Composed_of();
					
					// attribute 'Composed_of.unitPrice'
					Double firstNotNull_unitPrice = Util.getDoubleValue(r.getAs("unitPrice_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_unitPrice != null && unitPrice2 != null && !firstNotNull_unitPrice.equals(unitPrice2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_unitPrice == null && unitPrice2 != null) {
							firstNotNull_unitPrice = unitPrice2;
						}
					}
					composed_of_res.setUnitPrice(firstNotNull_unitPrice);
					
					// attribute 'Composed_of.quantity'
					Integer firstNotNull_quantity = Util.getIntegerValue(r.getAs("quantity_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer quantity2 = Util.getIntegerValue(r.getAs("quantity_" + i));
						if (firstNotNull_quantity != null && quantity2 != null && !firstNotNull_quantity.equals(quantity2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
							logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
						}
						if (firstNotNull_quantity == null && quantity2 != null) {
							firstNotNull_quantity = quantity2;
						}
					}
					composed_of_res.setQuantity(firstNotNull_quantity);
					
					// attribute 'Composed_of.discount'
					Double firstNotNull_discount = Util.getDoubleValue(r.getAs("discount_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double discount2 = Util.getDoubleValue(r.getAs("discount_" + i));
						if (firstNotNull_discount != null && discount2 != null && !firstNotNull_discount.equals(discount2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discount': " + firstNotNull_discount + " and " + discount2 + "." );
							logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discount': " + firstNotNull_discount + " and " + discount2 + "." );
						}
						if (firstNotNull_discount == null && discount2 != null) {
							firstNotNull_discount = discount2;
						}
					}
					composed_of_res.setDiscount(firstNotNull_discount);
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							composed_of_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							composed_of_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order order_res = new Order();
					Product product_res = new Product();
					
					// attribute 'Order.id'
					Integer firstNotNull_order_id = Util.getIntegerValue(r.getAs("order_0.id"));
					order_res.setId(firstNotNull_order_id);
					// attribute 'Order.freight'
					Double firstNotNull_order_freight = Util.getDoubleValue(r.getAs("order_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double order_freight2 = Util.getDoubleValue(r.getAs("order_" + i + ".freight"));
						if (firstNotNull_order_freight != null && order_freight2 != null && !firstNotNull_order_freight.equals(order_freight2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
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
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
						}
						if (firstNotNull_order_shippedDate == null && order_shippedDate2 != null) {
							firstNotNull_order_shippedDate = order_shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_order_shippedDate);
					// attribute 'Product.id'
					Integer firstNotNull_product_id = Util.getIntegerValue(r.getAs("product_0.id"));
					product_res.setId(firstNotNull_product_id);
					// attribute 'Product.name'
					String firstNotNull_product_name = Util.getStringValue(r.getAs("product_0.name"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_name2 = Util.getStringValue(r.getAs("product_" + i + ".name"));
						if (firstNotNull_product_name != null && product_name2 != null && !firstNotNull_product_name.equals(product_name2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
						}
						if (firstNotNull_product_name == null && product_name2 != null) {
							firstNotNull_product_name = product_name2;
						}
					}
					product_res.setName(firstNotNull_product_name);
					// attribute 'Product.supplierRef'
					Integer firstNotNull_product_supplierRef = Util.getIntegerValue(r.getAs("product_0.supplierRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_supplierRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".supplierRef"));
						if (firstNotNull_product_supplierRef != null && product_supplierRef2 != null && !firstNotNull_product_supplierRef.equals(product_supplierRef2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
						}
						if (firstNotNull_product_supplierRef == null && product_supplierRef2 != null) {
							firstNotNull_product_supplierRef = product_supplierRef2;
						}
					}
					product_res.setSupplierRef(firstNotNull_product_supplierRef);
					// attribute 'Product.categoryRef'
					Integer firstNotNull_product_categoryRef = Util.getIntegerValue(r.getAs("product_0.categoryRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_categoryRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".categoryRef"));
						if (firstNotNull_product_categoryRef != null && product_categoryRef2 != null && !firstNotNull_product_categoryRef.equals(product_categoryRef2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
						}
						if (firstNotNull_product_categoryRef == null && product_categoryRef2 != null) {
							firstNotNull_product_categoryRef = product_categoryRef2;
						}
					}
					product_res.setCategoryRef(firstNotNull_product_categoryRef);
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_product_quantityPerUnit = Util.getStringValue(r.getAs("product_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_quantityPerUnit2 = Util.getStringValue(r.getAs("product_" + i + ".quantityPerUnit"));
						if (firstNotNull_product_quantityPerUnit != null && product_quantityPerUnit2 != null && !firstNotNull_product_quantityPerUnit.equals(product_quantityPerUnit2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
						}
						if (firstNotNull_product_quantityPerUnit == null && product_quantityPerUnit2 != null) {
							firstNotNull_product_quantityPerUnit = product_quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_product_quantityPerUnit);
					// attribute 'Product.unitPrice'
					Double firstNotNull_product_unitPrice = Util.getDoubleValue(r.getAs("product_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double product_unitPrice2 = Util.getDoubleValue(r.getAs("product_" + i + ".unitPrice"));
						if (firstNotNull_product_unitPrice != null && product_unitPrice2 != null && !firstNotNull_product_unitPrice.equals(product_unitPrice2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
						}
						if (firstNotNull_product_unitPrice == null && product_unitPrice2 != null) {
							firstNotNull_product_unitPrice = product_unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_product_unitPrice);
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_product_reorderLevel = Util.getIntegerValue(r.getAs("product_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_reorderLevel2 = Util.getIntegerValue(r.getAs("product_" + i + ".reorderLevel"));
						if (firstNotNull_product_reorderLevel != null && product_reorderLevel2 != null && !firstNotNull_product_reorderLevel.equals(product_reorderLevel2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
						}
						if (firstNotNull_product_reorderLevel == null && product_reorderLevel2 != null) {
							firstNotNull_product_reorderLevel = product_reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_product_reorderLevel);
					// attribute 'Product.discontinued'
					Boolean firstNotNull_product_discontinued = Util.getBooleanValue(r.getAs("product_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean product_discontinued2 = Util.getBooleanValue(r.getAs("product_" + i + ".discontinued"));
						if (firstNotNull_product_discontinued != null && product_discontinued2 != null && !firstNotNull_product_discontinued.equals(product_discontinued2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
						}
						if (firstNotNull_product_discontinued == null && product_discontinued2 != null) {
							firstNotNull_product_discontinued = product_discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_product_discontinued);
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_product_unitsInStock = Util.getIntegerValue(r.getAs("product_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsInStock2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsInStock"));
						if (firstNotNull_product_unitsInStock != null && product_unitsInStock2 != null && !firstNotNull_product_unitsInStock.equals(product_unitsInStock2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_product_unitsInStock + " and " + product_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_product_unitsInStock + " and " + product_unitsInStock2 + "." );
						}
						if (firstNotNull_product_unitsInStock == null && product_unitsInStock2 != null) {
							firstNotNull_product_unitsInStock = product_unitsInStock2;
						}
					}
					product_res.setUnitsInStock(firstNotNull_product_unitsInStock);
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_product_unitsOnOrder = Util.getIntegerValue(r.getAs("product_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsOnOrder2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsOnOrder"));
						if (firstNotNull_product_unitsOnOrder != null && product_unitsOnOrder2 != null && !firstNotNull_product_unitsOnOrder.equals(product_unitsOnOrder2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_product_unitsOnOrder + " and " + product_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_product_unitsOnOrder + " and " + product_unitsOnOrder2 + "." );
						}
						if (firstNotNull_product_unitsOnOrder == null && product_unitsOnOrder2 != null) {
							firstNotNull_product_unitsOnOrder = product_unitsOnOrder2;
						}
					}
					product_res.setUnitsOnOrder(firstNotNull_product_unitsOnOrder);
	
					composed_of_res.setOrder(order_res);
					composed_of_res.setProduct(product_res);
					return composed_of_res;
		}
		, Encoders.bean(Composed_of.class));
	
	}
	
	//Empty arguments
	public Dataset<Composed_of> getComposed_ofList(){
		 return getComposed_ofList(null,null,null);
	}
	
	public abstract Dataset<Composed_of> getComposed_ofList(
		Condition<OrderAttribute> order_condition,
		Condition<ProductAttribute> product_condition,
		Condition<Composed_ofAttribute> composed_of_condition
	);
	
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
	
	public abstract void insertComposed_of(Composed_of composed_of);
	
	public 	abstract boolean insertComposed_ofInJoinStructOrder_DetailsInRelData(Composed_of composed_of);
	
	
	
	 public void insertComposed_of(Order order ,Product product ){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrder(order);
		composed_of.setProduct(product);
		insertComposed_of(composed_of);
	}
	
	 public void insertComposed_of(Product product, List<Order> orderList){
		Composed_of composed_of = new Composed_of();
		composed_of.setProduct(product);
		for(Order order : orderList){
			composed_of.setOrder(order);
			insertComposed_of(composed_of);
		}
	}
	 public void insertComposed_of(Order order, List<Product> productList){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrder(order);
		for(Product product : productList){
			composed_of.setProduct(product);
			insertComposed_of(composed_of);
		}
	}
	
	public abstract void updateComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	);
	
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
	
	public abstract void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition);
	
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
