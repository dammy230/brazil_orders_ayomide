import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import (
    Base, get_db, Sellers, Customers, Order_Items, Order_Payments, Orders,
    Products, Product_Category, FactTable, Top_Sellers)

class TestDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create an in-memory SQLite database for testing
        cls.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(cls.engine)
        cls.SessionLocal = sessionmaker(bind=cls.engine)

    def setUp(self):
        # Start a new session for each test
        self.session = self.SessionLocal()

    def tearDown(self):
        # Close the session after each test
        self.session.close()

    def test_get_db(self):
        with patch('src.database.create_engine') as mock_create_engine:
            mock_create_engine.return_value = self.engine
            with get_db() as db:
                self.assertIsNotNone(db)

    def test_sellers_model(self):
        seller = Sellers(seller_id="S001", seller_zip_code_prefix=12345, seller_city="Test City", seller_state="TS")
        self.session.add(seller)
        self.session.commit()

        queried_seller = self.session.query(Sellers).filter_by(seller_id="S001").first()
        self.assertIsNotNone(queried_seller)
        self.assertEqual(queried_seller.seller_city, "Test City")

    def test_customers_model(self):
        customer = Customers(customer_id="C001", customer_unique_id="CU001", customer_zip_code_prefix=54321, customer_city="Test City", customer_state="TS")
        self.session.add(customer)
        self.session.commit()

        queried_customer = self.session.query(Customers).filter_by(customer_id="C001").first()
        self.assertIsNotNone(queried_customer)
        self.assertEqual(queried_customer.customer_unique_id, "CU001")

    def test_order_items_model(self):
        order_item = Order_Items(order_id="O001", order_item_id=1, product_id="P001", seller_id="S001", price=100.0, freight_value=10.0)
        self.session.add(order_item)
        self.session.commit()

        queried_order_item = self.session.query(Order_Items).filter_by(order_id="O001").first()
        self.assertIsNotNone(queried_order_item)
        self.assertEqual(queried_order_item.price, 100.0)

    def test_order_payments_model(self):
        order_payment = Order_Payments(order_id="O001", payment_sequential=1, payment_type="credit_card", payment_installments=3, payment_value=100.0)
        self.session.add(order_payment)
        self.session.commit()

        queried_order_payment = self.session.query(Order_Payments).filter_by(order_id="O001").first()
        self.assertIsNotNone(queried_order_payment)
        self.assertEqual(queried_order_payment.payment_type, "credit_card")

    def test_orders_model(self):
        order = Orders(order_id="O001", customer_id="C001", order_status="delivered")
        self.session.add(order)
        self.session.commit()

        queried_order = self.session.query(Orders).filter_by(order_id="O001").first()
        self.assertIsNotNone(queried_order)
        self.assertEqual(queried_order.order_status, "delivered")

    def test_products_model(self):
        product = Products(product_id="P001", product_category_name="electronics", product_weight_g=1000)
        self.session.add(product)
        self.session.commit()

        queried_product = self.session.query(Products).filter_by(product_id="P001").first()
        self.assertIsNotNone(queried_product)
        self.assertEqual(queried_product.product_category_name, "electronics")

    def test_product_category_model(self):
        category = Product_Category(product_category_name="eletrônicos", product_category_name_english="electronics")
        self.session.add(category)
        self.session.commit()

        queried_category = self.session.query(Product_Category).filter_by(product_category_name="eletrônicos").first()
        self.assertIsNotNone(queried_category)
        self.assertEqual(queried_category.product_category_name_english, "electronics")

    def test_fact_table_model(self):
        fact = FactTable(order_id="O001", customer_id="C001", product_id="P001", seller_id="S001", price=100.0)
        self.session.add(fact)
        self.session.commit()

        queried_fact = self.session.query(FactTable).filter_by(order_id="O001").first()
        self.assertIsNotNone(queried_fact)
        self.assertEqual(queried_fact.price, 100.0)

    def test_top_sellers_model(self):
        top_seller = Top_Sellers(seller_id="S001", total_sales=10000.0)
        self.session.add(top_seller)
        self.session.commit()

        queried_top_seller = self.session.query(Top_Sellers).filter_by(seller_id="S001").first()
        self.assertIsNotNone(queried_top_seller)
        self.assertEqual(queried_top_seller.total_sales, 10000.0)

if __name__ == '__main__':
    unittest.main()