package com.demo.project.spark

case class OrderItems(
                       order_item_id: Int,
                       order_item_order_id: Int,
                       order_item_product_id: Int,
                       order_item_quantity: Int,
                       order_item_subtotal: Float,
                       order_item_product_price: Float)