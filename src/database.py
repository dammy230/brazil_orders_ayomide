from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Sequence
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_path = "customer_orders.duckdb"
Base = declarative_base()

@contextmanager
def get_db():
    sql_alchemy_database_url = f"duckdb:///{db_path}"
    engine = create_engine(sql_alchemy_database_url)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=create_engine(sql_alchemy_database_url))
    db = SessionLocal()
    try:
        logger.info("Database session opened")
        yield db
    except Exception as e:
        logger.error(f"Error occurred during database operation: {str(e)}")
        raise
    finally:
        logger.info("Closing database session")
        db.close()


class Sellers(Base):
    __tablename__ = "sellers"

    id = Column(Integer, Sequence('id'), primary_key=True)
    seller_id = Column(String)
    seller_zip_code_prefix = Column(Integer)
    seller_city = Column(String)
    seller_state = Column(String)

class Customers(Base):
    __tablename__ = "customers"

    id = Column(Integer, Sequence('id'), primary_key=True)
    customer_id = Column(String)
    customer_unique_id = Column(String)
    customer_zip_code_prefix = Column(Integer)
    customer_city = Column(String)
    customer_state = Column(String)

class Order_Items(Base):
    __tablename__ = "order_items"

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_id = Column(String)
    order_item_id = Column(Integer)
    product_id = Column(String)
    seller_id = Column(String)
    shipping_limit_date = Column(DateTime)
    price = Column(Float)
    freight_value = Column(Float)    

class Order_Payments(Base):
    __tablename__ = "order_payments"

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_id = Column(String)
    payment_sequential = Column(Integer)
    payment_type = Column(String)
    payment_installments = Column(Integer)
    payment_value = Column(Float)

class Orders(Base):
    __tablename__ = "orders"

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_id = Column(String)
    customer_id = Column(String)
    order_status = Column(String)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(DateTime)
    order_delivered_carrier_date = Column(DateTime)
    order_delivered_customer_date = Column(DateTime)
    order_estimated_delivery_date = Column(DateTime)


class Products(Base):
    __tablename__ = "products"
    id = Column(Integer, Sequence('id'), primary_key=True)
    product_id = Column(String)
    product_category_name = Column(String)
    product_name_lenght = Column(String)
    product_description_lenght = Column(String)
    product_photos_qty = Column(String)
    product_weight_g = Column(Integer)
    product_length_cm = Column(Integer)
    product_height_cm = Column(Integer)
    product_width_cm = Column(Integer)

class Product_Category(Base):
    __tablename__ = "product_category"

    id = Column(Integer, Sequence('id'), primary_key=True)
    product_category_name = Column(String)
    product_category_name_english = Column(String)


class FactTable(Base):
    __tablename__ = "fact_table"

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_id = Column(String)
    customer_id = Column(String)
    order_status = Column(String)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(DateTime)
    order_delivered_carrier_date = Column(DateTime)
    order_delivered_customer_date = Column(DateTime)
    order_estimated_delivery_date = Column(DateTime)
    customer_unique_id = Column(String)
    customer_zip_code_prefix = Column(Integer)
    customer_city = Column(String)
    customer_state = Column(String)
    order_item_id = Column(Integer)
    product_id = Column(String)
    seller_id = Column(String)
    shipping_limit_date = Column(DateTime)
    price = Column(Float)
    freight_value = Column(Float)
    product_category_name = Column(String)
    seller_zip_code_prefix = Column(Integer)
    seller_city = Column(String)
    seller_state = Column(String)
    product_category_name_english = Column(String)


class Top_Sellers(Base):
    __tablename__ = "top_sellers"

    id = Column(Integer, Sequence('id'), primary_key=True)
    seller_id = Column(String)
    total_sales = Column(Float)