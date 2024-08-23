from flask import Flask, request, jsonify
import polars
import pandas as pd
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from werkzeug.utils import secure_filename
from flask_swagger_ui import get_swaggerui_blueprint
from src.processing import (load_data,transform__df,process_fact_table,process_dim_table_df,get_top_sellers)
from sqlalchemy.orm import Session
from src.database import (get_db, Sellers,Customers,Orders,Order_Items,Products,Product_Category,FactTable,Order_Payments,Top_Sellers)

import polars as pl
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



app = Flask(__name__)

#configuring upload folder
app.config['UPLOAD_FOLDER'] = 'uploads'

#ensuring upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

#configuring swagger UI
SWAGGER_URL = '/api/docs'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "Brazillian Orders ETL API"}
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


def df_to_list_of_dicts(df):
    new_df_dict = {}
    all_records = []
    df_columns = df.columns
    for row in df.iter_rows():
        num = 0
        for col in df.columns:
            new_df_dict[col] = row[df_columns.index(col)]
            num += 1
            if num % len(df_columns) == 0:
                all_records.append(new_df_dict.copy()) #get the dataframe records into a list of dictionaries,
    
    return all_records

# list_dicts_df = df_to_list_of_dicts(df)
# json_df = jsonify(list_dicts_df)
# print(json_df)

@app.route('/api/load_sellers_data', methods=['POST'])
def api_load_sellers_data(
):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                # df = df.head(15000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                sellers_df_columns = ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']
                if not all(col in list_dicts_df[0] for col in sellers_df_columns):
                    return jsonify({'error': 'Invalid file, Kindly provide the Sellers csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        seller_ids = tuple(record['id'] for record in list_dicts_df)
                        # Use a single query to check for existing sellers
                        existing_sellers = db.query(Sellers).filter(Sellers.id.in_(seller_ids)).all()
                        print("existing_sellers", existing_sellers)
                        logger.info(f"Found {len(existing_sellers)} existing sellers")
                        # Create a set of existing seller IDs for faster lookup
                        existing_seller_ids = {seller.id for seller in existing_sellers}
                        # Insert only new sellers
                        new_sellers = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_seller_ids:
                                seller = Sellers(
                                    id = record['id'],
                                    seller_id=record['seller_id'],
                                    seller_zip_code_prefix=record['seller_zip_code_prefix'],
                                    seller_city=record['seller_city'],
                                    seller_state=record['seller_state']
                                )
                                new_sellers.append(seller)
                                db.add(seller)
                                db.commit()
                                db.refresh(seller)
                        
                        sellers_list = []
                        
                        for seller in new_sellers:
                            sellers_dict = {}
                            sellers_dict['id'] = seller.id
                            sellers_dict['seller_id'] = seller.seller_id
                            sellers_dict['seller_zip_code_prefix'] = seller.seller_zip_code_prefix
                            sellers_dict['seller_city'] = seller.seller_city
                            sellers_dict['seller_state'] = seller.seller_state
                            sellers_list.append(sellers_dict)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': sellers_list[:5],
                            'new_sellers_count': len(new_sellers),
                            'existing_sellers_count': len(existing_sellers)
                        }), 200
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid sellers dataset: {str(e)}")
                return jsonify({'status':'error',
                                'error': str(e)}), 500
                    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status':'error',
                        'error': str(e)}), 500
    

@app.route('/api/get_sellers', methods=['GET'])
def api_get_sellers():
    with get_db() as db:
        try:
            sellers = db.query(Sellers).all()
            sellers_filtered= sellers[:5]
            print("all sellers, ", sellers_filtered)
            sellers_list = []
            if sellers_filtered:
                for seller in sellers_filtered:
                    sellers_list.append({
                        'id': seller.id,
                        'seller_id': seller.seller_id,
                        'seller_zip_code_prefix': seller.seller_zip_code_prefix,
                        'seller_city': seller.seller_city,
                        'seller_state': seller.seller_state
                    })

                return jsonify({"status": "success",
                                "message": "Sellers data retrieved successfully",
                                "body":  sellers_list,
                                "count_records": len(sellers) }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Sellers data available",
                                "body":  sellers_list
                            }), 200
        
        except Exception as e:
            logger.error(f"An error occurred retrieving all brands: {str(e)}")
            
            return jsonify({'status':  'error',
                            'error': f'retrieving sellers data: {str(e)}'}), 500


@app.route('/api/load_customers_data', methods=['POST'])
def api_load_customers_data(
):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                df = df.head(10000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                customers_df_columns = ['customer_id', 'customer_unique_id', 
                                      'customer_zip_code_prefix', 'customer_city',
                                      'customer_state']
                
                if not all(col in list_dicts_df[0] for col in customers_df_columns):
                    return jsonify({'error': 'Invalid file, Kindly provide the Customers csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        customer_ids = tuple(record['id'] for record in list_dicts_df)
                        # Use a single query to check for existing sellers
                        existing_customers = db.query(Customers).filter(Customers.id.in_(customer_ids)).all()
                        # print("existing_customers", existing_customers)
                        logger.info(f"Found {len(existing_customers)} existing existing_customers")
                        # Create a set of existing seller IDs for faster lookup
                        existing_customer_ids = {customer.id for customer in existing_customers}
                        # Insert only new sellers
                        new_customers = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_customer_ids:
                                customer = Customers(
                                    id = record['id'],
                                    customer_id=record['customer_id'],
                                    customer_unique_id=record['customer_unique_id'],
                                    customer_zip_code_prefix=record['customer_zip_code_prefix'],
                                    customer_city=record['customer_city'],
                                    customer_state = record['customer_state']
                                )
                                new_customers.append(customer)
                                db.add(customer)
                                db.commit()
                                db.refresh(customer)
                        
                        customers_list = []
                        
                        for customer in new_customers:
                            customers_dict = {}
                            customers_dict['id'] = customer.id
                            customers_dict['customer_id'] = customer.customer_id
                            customers_dict['customer_unique_id'] = customer.customer_unique_id
                            customers_dict['customer_zip_code_prefix'] = customer.customer_zip_code_prefix
                            customers_dict['customer_city'] = customer.customer_city
                            customers_dict['customer_state'] = customer.customer_state
                            customers_list.append(customers_dict)
                        
                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': customers_list[:5],
                            'new_customers_count': len(new_customers),
                            'existing_customers_count': len(existing_customers)
                        }), 200
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid customers dataset: {str(e)}")
                return jsonify({'status':'error','error': str(e)}), 500
                    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500
                    
@app.route('/api/get_customers', methods=['GET'])
def api_get_customers():
    with get_db() as db:
        try:
            customers = db.query(Customers).all()
            customers_fitered= customers[:5]
            # print("all customers, ", customers_fitered)
            customers_list = []
            if customers_fitered:
                for customer in customers_fitered:
                    customers_list.append({
                        'id': customer.id,
                        'customer_id': customer.customer_id,
                        'customer_unique_id': customer.customer_unique_id,
                        'customer_zip_code_prefix': customer.customer_zip_code_prefix,
                        'customer_city': customer.customer_city,
                        'customer_state': customer.customer_state
                    })

                return jsonify({"status": "success",
                                "message": "Customers data retrieved successfully",
                                "body":  customers_list,
                                "count": len(customers)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Customers data available",
                                "body":  customers_list
                            }), 200
        
        except Exception as e:
            logger.error(f"An error occurred retrieving all customers: {str(e)}")
            return jsonify({'status': 'error',
                            'error':f' retrieving Customers data: {str(e)}'}), 500


@app.route('/api/load_orders_data', methods=['POST'])
def api_load_orders_data(
):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                df = df.head(10000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                orders_df_columns = ['order_id', 'order_status', 
                                      'order_purchase_timestamp',
                                      'order_approved_at', 'order_delivered_carrier_date',
                                      'order_delivered_customer_date', 'order_estimated_delivery_date']
                
                if not all(col in list_dicts_df[0] for col in orders_df_columns):
                    return jsonify({'status':'error', 
                    'error': 'Invalid file, Kindly provide the Orders csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        order_ids = tuple(record['id'] for record in list_dicts_df)
                        # Use a single query to check for existing sellers
                        existing_orders = db.query(Orders).filter(Orders.id.in_(order_ids)).all()
                        # print("existing_orders", existing_orders)
                        logger.info(f"Found {len(existing_orders)} existing existing_orders")
                        # Create a set of existing seller IDs for faster lookup
                        existing_order_ids = {order.id for order in existing_orders}
                        # Insert only new sellers
                        new_orders = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_order_ids:
                                order = Orders(
                                    id = record['id'],
                                    order_id=record['order_id'],
                                    customer_id=record['customer_id'],
                                    order_status=record['order_status'],
                                    order_purchase_timestamp=record['order_purchase_timestamp'],
                                    order_approved_at = record['order_approved_at'],
                                    order_delivered_carrier_date = record['order_delivered_carrier_date'],
                                    order_delivered_customer_date = record['order_delivered_customer_date'],
                                    order_estimated_delivery_date = record['order_estimated_delivery_date']
                                )
                                new_orders.append(order)
                                db.add(order)
                                db.commit()
                                db.refresh(order)

                        order_list = []
                        for order in new_orders:
                            order_dict = {}
                            order_dict['id'] = order.id
                            order_dict['order_id'] = order.order_id
                            order_dict['customer_id'] = order.customer_id
                            order_dict['order_status'] = order.order_status
                            order_dict['order_purchase_timestamp'] = order.order_purchase_timestamp
                            order_dict['order_approved_at'] = order.order_approved_at
                            order_dict['order_delivered_carrier_date'] = order.order_delivered_carrier_date
                            order_dict['order_delivered_customer_date'] = order.order_delivered_customer_date
                            order_dict['order_estimated_delivery_date'] = order.order_estimated_delivery_date
                            order_list.append(order_dict)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': order_list[:5],
                            'new_orders_count': len(new_orders),
                            'existing_orders_count': len(existing_orders),
                            
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'status': 'error',
                                        'error': f'Database operation failed details {str(db_error)}'}), 500
                    
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid orders dataset: {str(e)}")
                return jsonify({'status': 'error', 'error': str(e)}), 500
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_orders', methods=['GET'])
def api_get_orders():
    with get_db() as db:
        try:
            orders = db.query(Orders).all()
            orders_filtered= orders[:5]
            orders_list = []
            if orders_filtered:
                for order in orders_filtered:
                    orders_list.append({
                        'id': order.id,
                        'order_id': order.order_id,
                        'order_status': order.order_status,
                        'order_purchase_timestamp': order.order_purchase_timestamp,
                        'order_approved_at': order.order_approved_at,
                        'order_delivered_carrier_date': order.order_delivered_carrier_date,
                        'order_delivered_customer_date': order.order_delivered_customer_date,
                        'order_estimated_delivery_date': order.order_estimated_delivery_date
                    })

                return jsonify({"status": "success",
                                "message": "Orders data retrieved successfully",
                                "body":  orders_list,
                                "count_records": len(orders)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Orders data available",
                                "body":  orders_list
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all orders: {str(e)}")
            return jsonify({f'status': 'error', 'error': f'error retrieving Orders data: {str(e)}'}), 500
        

@app.route('/api/load_order_items_data', methods=['POST'])
def api_load_order_items_data(
):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                df = df.head(10000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                order_items_df_columns = ['order_id', 'order_item_id', 'product_id', 
                                      'seller_id', 'shipping_limit_date', 'price', 'freight_value']
                
                if not all(col in list_dicts_df[0] for col in order_items_df_columns):
                    return jsonify({'status':'error','error': 'Invalid file, Kindly provide the Order Items csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        order_ids = tuple(record['id'] for record in list_dicts_df)
                        # Use a single query to check for existing orders
                        existing_order_items = db.query(Order_Items).filter(Order_Items.id.in_(order_ids)).all()
                        print("existing_order_items", existing_order_items)
                        logger.info(f"Found {len(existing_order_items)} existing existing_order_items")
                        # Create a set of existing order IDs for faster lookup
                        existing_order_ids = {order_item.id for order_item in existing_order_items}
                        # Insert only new orders
                        new_order_items = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_order_ids:
                                order_item = Order_Items(
                                    id = record['id'],
                                    order_id=record['order_id'],
                                    order_item_id=record['order_item_id'],
                                    product_id=record['product_id'],
                                    seller_id = record['seller_id'],
                                    shipping_limit_date = record['shipping_limit_date'],
                                    price = record['price'],
                                    freight_value = record['freight_value']
                                )
                                new_order_items.append(order_item)
                                db.add(order_item)
                                db.commit()
                                db.refresh(order_item)

                        order_items_list = []
                        
                        for order_item in new_order_items:
                            order_items_dict = {}
                            order_items_dict['id'] = order_item.id
                            order_items_dict['order_id'] = order_item.order_id
                            order_items_dict['order_item_id'] = order_item.order_item_id
                            order_items_dict['product_id'] = order_item.product_id
                            order_items_dict['seller_id'] = order_item.seller_id
                            order_items_dict['shipping_limit_date'] = order_item.shipping_limit_date
                            order_items_dict['price'] = order_item.price
                            order_items_dict['freight_value'] = order_item.freight_value
                            order_items_list.append(order_items_dict)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': order_items_list[:5],
                            'new_order_items_count': len(new_order_items),
                            'existing_order_items_count': len(existing_order_items)
                        }), 200
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'status':'error', 'error': 'Database operation failed', 'details': str(db_error)}), 500
            
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid order items dataset: {str(e)}")
                return jsonify({'status':'error', 
                
                            'error': str(e)}), 500
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status': 'error', 
        
            'error': str(e)}), 500


@app.route('/api/get_order_items', methods=['GET'])
def api_get_order_items():
    with get_db() as db:
        try:
            order_items = db.query(Order_Items).all()
            order_items_filtered= order_items[:5]
            order_items_list = []
            if order_items_filtered:
                for order_item in order_items_filtered:
                    order_items_list.append({
                        'id': order_item.id,
                        'order_id': order_item.order_id,
                        'order_item_id': order_item.order_item_id,
                        'product_id': order_item.product_id,
                        'seller_id': order_item.seller_id,
                        'shipping_limit_date': order_item.shipping_limit_date,
                        'price': order_item.price,
                        'freight_value': order_item.freight_value
                    })

                return jsonify({"status": "success",
                                "message": "Order Items data retrieved successfully",
                                "body":  order_items_list,
                                "count_records": len(order_items)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Order Items data available",
                                "body":  order_items_list
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all order items: {str(e)}")
        return jsonify({'status':'error', 
                        
                        'error':f'error retrieving Order Items data: {str(e)}'}), 500


@app.route('/api/load_order_payments_data', methods=['POST'])
def api_load_order_payments_data(
):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                df = df.head(10000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                order_payments_df_columns = ['order_id', 'payment_sequential', 'payment_type', 
                                      'payment_installments', 'payment_value']
                
                if not all(col in list_dicts_df[0] for col in order_payments_df_columns):
                    return jsonify({'error': 'Invalid file, Kindly provide the Order Payments csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        order_ids = tuple(record['id'] for record in list_dicts_df)
                        # Use a single query to check for existing orders
                        existing_order_payments = db.query(Order_Payments).filter(Order_Payments.id.in_(order_ids)).all()
                        print("existing_order_payments", existing_order_payments)
                        logger.info(f"Found {len(existing_order_payments)} existing existing_order_payments")
                        # Create a set of existing order IDs for faster lookup
                        existing_order_ids = {order_payment.id for order_payment in existing_order_payments}
                        # Insert only new orders
                        new_order_payments = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_order_ids:
                                order_payment = Order_Payments(
                                    id = record['id'],
                                    order_id=record['order_id'],
                                    payment_sequential=record['payment_sequential'],
                                    payment_type=record['payment_type'],
                                    payment_installments = record['payment_installments'],
                                    payment_value = record['payment_value']
                                )
                                new_order_payments.append(order_payment)
                                db.add(order_payment)
                                db.commit()
                                db.refresh(order_payment)

                        order_payments_list = []
                        
                        for order_payment in new_order_payments:
                            order_payments_dict = {}
                            order_payments_dict['id'] = order_payment.id
                            order_payments_dict['order_id'] = order_payment.order_id
                            order_payments_dict['payment_sequential'] = order_payment.payment_sequential
                            order_payments_dict['payment_type'] = order_payment.payment_type
                            order_payments_dict['payment_installments'] = order_payment.payment_installments
                            order_payments_dict['payment_value'] = order_payment.payment_value
                            order_payments_list.append(order_payments_dict)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': order_payments_list[:5],
                            'new_order_payments_count': len(new_order_payments),
                            'existing_order_payments_count': len(existing_order_payments)
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'status':'error', 'error': f'Database operation failed details: {str(db_error)}'}), 500
                    
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid order payments dataset: {str(e)}")
                return jsonify({'status':'error', 'error': str(e)}), 500
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status':'error', 'error': str(e)}), 500


@app.route('/api/get_order_payments', methods=['GET'])
def api_get_order_payments():
    with get_db() as db:
        try:
            order_payments = db.query(Order_Payments).all()
            order_payments_filtered = order_payments[:5]
            order_payments_list = []
            if order_payments_filtered:
                for order_payment in order_payments_filtered:
                    order_payments_list.append({
                        'id': order_payment.id,
                        'order_id': order_payment.order_id,
                        'payment_sequential': order_payment.payment_sequential,
                        'payment_type': order_payment.payment_type,
                        'payment_installments': order_payment.payment_installments,
                        'payment_value': order_payment.payment_value
                    })

                return jsonify({"status": "success",
                                "message": "Order Payments data retrieved successfully",
                                "body":  order_payments_list,
                                "count_records": len(order_payments)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Order Payments data available",
                                "body":  order_payments_list
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all order payments: {str(e)}")
            return jsonify({'status':'error', 'error':f'error retrieving Order Payments data {str(e)}'}), 500


@app.route('/api/load_products_data', methods=['POST'])
def api_load_products_data(
):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                df = df.head(10000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                products_df_columns = ['product_id', 'product_category_name', 'product_name_lenght', 
                                      'product_description_lenght', 'product_photos_qty', 'product_weight_g',
                                      'product_length_cm', 'product_height_cm', 'product_width_cm']
                
                if not all(col in list_dicts_df[0] for col in products_df_columns):
                    return jsonify({'error': 'Invalid file, Kindly provide the Products csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        product_ids = tuple(record['id'] for record in list_dicts_df)
                        # Use a single query to check for existing products
                        existing_products = db.query(Products).filter(Products.id.in_(product_ids)).all()
                        print("existing_products", existing_products)
                        logger.info(f"Found {len(existing_products)} existing existing_products")
                        # Create a set of existing product IDs for faster lookup
                        existing_product_ids = {product.id for product in existing_products}
                        # Insert only new products
                        new_products = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_product_ids:
                                product = Products(
                                    id = record['id'],
                                    product_id=record['product_id'],
                                    product_category_name=record['product_category_name'],
                                    product_name_lenght=record['product_name_lenght'],
                                    product_description_lenght = record['product_description_lenght'],
                                    product_photos_qty = record['product_photos_qty'],
                                    product_weight_g = record['product_weight_g'],
                                    product_length_cm = record['product_length_cm'],
                                    product_height_cm = record['product_height_cm'],
                                    product_width_cm = record['product_width_cm']
                                )

                                new_products.append(product)
                                db.add(product)
                                db.commit()
                                db.refresh(product)

                        products_list = []
                        
                        for product in new_products:
                            products_dict = {}
                            products_dict['id'] = product.id
                            products_dict['product_id'] = product.product_id
                            products_dict['product_category_name'] = product.product_category_name
                            products_dict['product_name_lenght'] = product.product_name_lenght
                            products_dict['product_description_lenght'] = product.product_description_lenght
                            products_dict['product_photos_qty'] = product.product_photos_qty
                            products_dict['product_weight_g'] = product.product_weight_g
                            products_dict['product_length_cm'] = product.product_length_cm
                            products_dict['product_height_cm'] = product.product_height_cm
                            products_dict['product_width_cm'] = product.product_width_cm
                            products_list.append(products_dict)
                        # print("products_list", products_list)           
                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': products_list[:5],
                            'new_products_count': len(new_products),
                            'existing_products_count': len(existing_products)
                        }), 200

                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid products dataset: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_products', methods=['GET'])
def api_get_products():
    with get_db() as db:
        try:
            products = db.query(Products).all()
            products_filtered= products[:5]
            products_list = []
            if products_filtered:
                for product in products_filtered:
                    products_list.append({
                        'id': product.id,
                        'product_id': product.product_id,
                        'product_category_name': product.product_category_name,
                        'product_name_lenght': product.product_name_lenght,
                        'product_description_lenght': product.product_description_lenght,
                        'product_photos_qty': product.product_photos_qty,
                        'product_weight_g': product.product_weight_g,
                        'product_length_cm': product.product_length_cm,
                        'product_height_cm': product.product_height_cm,
                        'product_width_cm': product.product_width_cm
                    })

                return jsonify({"status": "success",
                                "message": "Products data retrieved successfully",
                                "body":  products_list,
                                "count_records": len(products)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Products data available",
                                "body":  products_list,
                                "count_records": len(products)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all products: {str(e)}")
            return jsonify({'error retrieving Products data': str(e)}), 500


@app.route('/api/load_products_category', methods=['POST'])
def api_load_products_category():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'})
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                df = load_data(file_path)
                df = df.head(10000)
                df = transform__df(df)
                print("df", df.head())
                list_dicts_df = df_to_list_of_dicts(df)
                #if list_dicts_df do not have specific columns, return error
                product_category_df_columns = ['product_category_name', 'product_category_name_english']
                
                if not all(col in list_dicts_df[0] for col in product_category_df_columns):
                    return jsonify({'error': 'Invalid file, Kindly provide the Product Category csv file'
                                    }), 400

                with get_db() as db:
                    try:
                        # Convert list of dictionaries to a list of tuples for better performance
                        product_category_ids = tuple(record['id'] for record in list_dicts_df)

                        print("product_category_ids", product_category_ids)
                        # Use a single query to check for existing product categories
                        existing_product_categories = db.query(Product_Category).filter(Product_Category.id.in_(product_category_ids)).all()
                        print("existing_product_categories", existing_product_categories)
                        logger.info(f"Found {len(existing_product_categories)} existing existing_product_categories")
                        # Create a set of existing product category names for faster lookup
                        existing_product_category_names = {product.id for product in existing_product_categories}
                        # print("existing_product_category_names", existing_product_category_names)
                        # Insert only new product categories
                        new_product_categories = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_product_category_names:
                                product_category = Product_Category(
                                    id = record['id'],
                                    product_category_name=record['product_category_name'],
                                    product_category_name_english=record['product_category_name_english']
                                )
                                new_product_categories.append(product_category)
                                db.add(product_category)
                                db.commit()
                                db.refresh(product_category)
                        
                        print("new_product_categories", new_product_categories)

                        product_category_list = []
                        for product_category in new_product_categories:
                            product_category_dict = {}
                            product_category_dict['id'] = product_category.id
                            product_category_dict['product_category_name'] = product_category.product_category_name
                            product_category_dict['product_category_name_english'] = product_category.product_category_name_english
                            product_category_list.append(product_category_dict)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': product_category_list[:5],
                            'new_product_categories_count': len(new_product_categories),
                            'existing_product_categories_count': len(existing_product_categories)
                        }), 200

                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500   
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid product category dataset: {str(e)}")
                return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_products_category', methods=['GET'])
def api_get_products_category():
    with get_db() as db:
        try:
            product_categories = db.query(Product_Category).all()
            product_categories_filtered= product_categories[:5]
            product_categories_list = []
            if product_categories_filtered:
                for product_category in product_categories_filtered:
                    product_categories_list.append({
                        'id': product_category.id,
                        'product_category_name': product_category.product_category_name,
                        'product_category_name_english': product_category.product_category_name_english
                    })

                return jsonify({"status": "success",
                                "message": "Product Categories data retrieved successfully",
                                "body":  product_categories_list,
                                "count_records": len(product_categories)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Product Categories data available",
                                "body":  product_categories_list,
                                "count_records": len(product_categories)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all product categories: {str(e)}")
            return jsonify({'error retrieving Product Categories data': str(e)}), 500


@app.route('/api/process_fact_table', methods=['POST'])
def api_process_fact_table():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                sellers = db.query(Sellers).all()
                customers = db.query(Customers).all()
                orders = db.query(Orders).all()
                order_items = db.query(Order_Items).all()
                order_payments = db.query(Order_Payments).all()
                products = db.query(Products).all()
                product_categories = db.query(Product_Category).all()
                
                # Check if the tables are not empty
                if not sellers or not customers or not orders \
                    or not order_items or not order_payments or not products \
                        or not product_categories:
                    return jsonify({'error': 'One or more of the dimension tables are empty'}), 404
                
                orders_df = process_dim_table_df(orders)
                order_items_df = process_dim_table_df(order_items)
                customers_df = process_dim_table_df(customers)
                order_payments_df = process_dim_table_df(order_payments)
                products_df = process_dim_table_df(products)
                sellers_df = process_dim_table_df(sellers)
                product_categories_df = process_dim_table_df(product_categories)

                # print("orders", orders)

                list_of_tables = [orders_df, order_items_df, customers_df, 
                                      order_payments_df,products_df, 
                                  sellers_df, product_categories_df]  
                

                for df in list_of_tables:
                    if not isinstance(df, pl.DataFrame):
                        return jsonify({'status': 'error',
                                        'body': {df.head(1).to_dicts()} ,
                                        'message': f'transformed_data didnt yield a valid polars dataframe'}), 400

                fact_table = process_fact_table(list_of_tables)
                #remove duplicates 
                fact_table = fact_table.filter(fact_table.is_duplicated() == False)
                print("fact_table", fact_table)

                if not isinstance(fact_table, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'fact_table didnt yield a valid polars dataframe'}), 400

                list_dicts_df = df_to_list_of_dicts(fact_table)

                fact_table_ids = [record['id'] for record in list_dicts_df]
                existing_fact_table = db.query(FactTable).filter(FactTable.id.in_(fact_table_ids)).all()
                logger.info(f"Found {len(existing_fact_table)} existing fact_table")
                # Create a set of existing fact_table IDs for faster lookup
                existing_fact_table_ids = {fact.id for fact in existing_fact_table}
                # Insert only new fact_table
                new_fact_table = []
                for record in list_dicts_df:
                    if record['id'] not in existing_fact_table_ids:
                        fact = FactTable(
                            id = record['id'],
                            order_id=record['order_id'],
                            customer_id=record['customer_id'],
                            order_status=record['order_status'],
                            order_purchase_timestamp=record['order_purchase_timestamp'],
                            order_approved_at = record['order_approved_at'],
                            order_delivered_carrier_date = record['order_delivered_carrier_date'],
                            order_delivered_customer_date = record['order_delivered_customer_date'],
                            order_estimated_delivery_date = record['order_estimated_delivery_date'],
                            customer_unique_id = record['customer_unique_id'],
                            customer_zip_code_prefix = record['customer_zip_code_prefix'],
                            customer_city = record['customer_city'],
                            customer_state = record['customer_state'],
                            order_item_id = record['order_item_id'],
                            product_id = record['product_id'],
                            seller_id = record['seller_id'],
                            shipping_limit_date = record['shipping_limit_date'],
                            price = record['price'],
                            freight_value = record['freight_value'],
                            product_category_name = record['product_category_name'],
                            seller_zip_code_prefix = record['seller_zip_code_prefix'],
                            seller_city = record['seller_city'],
                            seller_state = record['seller_state'],
                            product_category_name_english = record['product_category_name_english']
                        )
                        new_fact_table.append(fact)
                        db.add(fact)
                        db.commit()
                        db.refresh(fact)
                    
                fact_table_list = []
                for fact in new_fact_table:
                    fact_dict = {}
                    fact_dict['id'] = fact.id
                    fact_dict['order_id'] = fact.order_id
                    fact_dict['customer_id'] = fact.customer_id
                    fact_dict['order_status'] = fact.order_status
                    fact_dict['order_purchase_timestamp'] = fact.order_purchase_timestamp
                    fact_dict['order_approved_at'] = fact.order_approved_at
                    fact_dict['order_delivered_carrier_date'] = fact.order_delivered_carrier_date
                    fact_dict['order_delivered_customer_date'] = fact.order_delivered_customer_date
                    fact_dict['order_estimated_delivery_date'] = fact.order_estimated_delivery_date
                    fact_dict['customer_unique_id'] = fact.customer_unique_id
                    fact_dict['customer_zip_code_prefix'] = fact.customer_zip_code_prefix
                    fact_dict['customer_city'] = fact.customer_city
                    fact_dict['customer_state'] = fact.customer_state
                    fact_dict['order_item_id'] = fact.order_item_id
                    fact_dict['product_id'] = fact.product_id
                    fact_dict['seller_id'] = fact.seller_id
                    fact_dict['shipping_limit_date'] = fact.shipping_limit_date
                    fact_dict['price'] = fact.price
                    fact_dict['freight_value'] = fact.freight_value
                    fact_dict['product_category_name'] = fact.product_category_name
                    fact_dict['seller_zip_code_prefix'] = fact.seller_zip_code_prefix
                    fact_dict['seller_city'] = fact.seller_city
                    fact_dict['seller_state'] = fact.seller_state
                    fact_dict['product_category_name_english'] = fact.product_category_name_english

                    fact_table_list.append(fact_dict)

                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': fact_table_list[:5],
                    'new_fact_table_count': len(new_fact_table),
                    'existing_fact_table_count': len(existing_fact_table)
                }), 200
            
            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/get_fact_table', methods=['GET'])
def api_get_fact_table():
    with get_db() as db:
        try:
            fact_table = db.query(FactTable).all()
            fact_table_filtered= fact_table[:5]
            fact_table_list = []
            if fact_table_filtered:
                for fact in fact_table_filtered:
                    fact_table_list.append({
                        'id': fact.id,
                        'order_id': fact.order_id,
                        'customer_id': fact.customer_id,
                        'order_status': fact.order_status,
                        'order_purchase_timestamp': fact.order_purchase_timestamp,
                        'order_approved_at': fact.order_approved_at,
                        'order_delivered_carrier_date': fact.order_delivered_carrier_date,
                        'order_delivered_customer_date': fact.order_delivered_customer_date,
                        'order_estimated_delivery_date': fact.order_estimated_delivery_date,
                        'customer_unique_id': fact.customer_unique_id,
                        'customer_zip_code_prefix': fact.customer_zip_code_prefix,
                        'customer_city': fact.customer_city,
                        'customer_state': fact.customer_state,
                        'order_item_id': fact.order_item_id,
                        'product_id': fact.product_id,
                        'seller_id': fact.seller_id,
                        'shipping_limit_date': fact.shipping_limit_date,
                        'price': fact.price,
                        'freight_value': fact.freight_value,
                        'product_category_name': fact.product_category_name,
                        'seller_zip_code_prefix': fact.seller_zip_code_prefix,
                        'seller_city': fact.seller_city,
                        'seller_state': fact.seller_state,
                        'product_category_name_english': fact.product_category_name_english
                    })

                return jsonify({"status": "success",
                                "message": "Fact Table data retrieved successfully",
                                "body":  fact_table_list,
                                "count_records": len(fact_table)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Fact Table data available",
                                "body":  fact_table_list,
                                "count_records": len(fact_table)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all fact table: {str(e)}")
            return jsonify({'error retrieving Fact Table data': str(e)}), 500

@app.route('/api/load_top_sellers', methods=['POST'])
def api_load_top_sellers():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                fact_table = db.query(FactTable).all()

                if not fact_table:
                    return jsonify({'error': 'Fact table to be analyzed not found in the database'}), 404
                
                fact_table = process_dim_table_df(fact_table)

                top_sellers_df = get_top_sellers(fact_table) 

                if not isinstance(top_sellers_df, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'transformed_data didnt yield a valid polars dataframe'}), 400
                
                list_dicts_df = df_to_list_of_dicts(top_sellers_df)

                top_sellers_ids = [record['id'] for record in list_dicts_df]
                existing_top_sellers = db.query(Top_Sellers).filter(Top_Sellers.id.in_(top_sellers_ids)).all()
                logger.info(f"Found {len(existing_top_sellers)} existing top_sellers")
                # Create a set of existing top_sellers IDs for faster lookup
                existing_top_sellers_ids = {top.id for top in existing_top_sellers}
                # Insert only new top_sellers
                new_top_sellers = []
                for record in list_dicts_df:
                    if record['id'] not in existing_top_sellers_ids:
                        top_seller = Top_Sellers(
                            id = record['id'],
                            seller_id=record['seller_id'],
                            total_sales = record['Total_sales'],
                        )
                        new_top_sellers.append(top_seller)
                        db.add(top_seller)
                        db.commit()
                        db.refresh(top_seller)
                
                top_sellers_list = []
                for top_seller in new_top_sellers:
                    top_seller_dict = {}
                    top_seller_dict['id'] = top_seller.id
                    top_seller_dict['seller_id'] = top_seller.seller_id
                    top_seller_dict['total_sales'] = top_seller.total_sales

                    top_sellers_list.append(top_seller_dict)
                
                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': top_sellers_list[:5],
                    'new_top_sellers_count': len(new_top_sellers),
                    'existing_top_sellers_count': len(existing_top_sellers)
                }), 200
            
            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_top_sellers', methods=['GET'])
def api_get_top_sellers():
    with get_db() as db:
        try:
            top_sellers = db.query(Top_Sellers).all()
            top_sellers_filtered= top_sellers[:5]
            top_sellers_list = []
            if top_sellers_filtered:
                for top_seller in top_sellers_filtered:
                    top_sellers_list.append({
                        'id': top_seller.id,
                        'seller_id': top_seller.seller_id,
                        'total_sales': top_seller.total_sales
                    })

                return jsonify({"status": "success",
                                "message": "Top Sellers data retrieved successfully",
                                "body":  top_sellers_list,
                                "count_records": len(top_sellers)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Top Sellers data available",
                                "body":  top_sellers_list,
                                "count_records": len(top_sellers)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all top sellers: {str(e)}")
            return jsonify({'error retrieving Top Sellers data': str(e)}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=7000)

