#adding the project root to the python path
# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest 
from unittest.mock import patch, MagicMock
from io import BytesIO
import json
from src.api import (app, get_db, Sellers,
                     Customers,
                     Orders,
                        Order_Items,
                        Order_Payments
                     )

class TestLoadSellers(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.load_data')
    @patch('src.api.transform__df')
    @patch('src.api.df_to_list_of_dicts')
    def test_with_sample_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
        #mocking the database funvtions
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        # Mocking database query results
        mock_session.query.return_value.filter.return_value.all.return_value = []
        
        #Mocking the functions that are called in the api
        mock_load_data.return_value = mock_session
        mock_transform__df.return_value = mock_session
        mock_df_to_list_of_dicts.return_value = [
            {'id': '1', 'seller_id': 'seller1', 'seller_zip_code_prefix': '12345', 'seller_city': 'City1', 'seller_state': 'State1'},
            {'id': '2', 'seller_id': 'seller2', 'seller_zip_code_prefix': '67890', 'seller_city': 'City2', 'seller_state': 'State2'}
        ]

        mock_file = (BytesIO(b'mock,file,content'), 'sellers.csv')

        response = self.app.post('/api/load_sellers_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_sellers_count'], 2)
        self.assertEqual(data['existing_sellers_count'], 0)

    def test_api_load_sellers_data_no_file(self):
        response = self.app.post('/api/load_sellers_data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'No file part')

    @patch('src.api.get_db')
    @patch('src.api.load_data')
    @patch('src.api.transform__df')
    @patch('src.api.df_to_list_of_dicts')
    def test_with_invalid_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
        mock_df_to_list_of_dicts.return_value = [{'invalid_column': 'data'}]
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_sellers.csv')
        response = self.app.post('/api/load_sellers_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Sellers csv file')


class TestGetSellers(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_get_sellers(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_sellers = [
            Sellers(id=1, seller_id='seller1', seller_zip_code_prefix='12345', seller_city='City1', seller_state='State1'),
            Sellers(id=2, seller_id='seller2', seller_zip_code_prefix='67890', seller_city='City2', seller_state='State2')
        ]
        mock_session.query.return_value.all.return_value = mock_sellers

        response = self.app.get('/api/get_sellers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Sellers data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['seller_id'], 'seller1')
        self.assertEqual(data['body'][1]['seller_id'], 'seller2')

    @patch('src.api.get_db')
    def test_get_sellers_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_sellers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Sellers data available")
        self.assertEqual(len(data['body']), 0)

    @patch('src.api.get_db')
    def test_get_sellers_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_sellers')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

class TestLoadCustomers(unittest.TestCase):
    
        def setUp(self):
            self.app = app.test_client()
            self.app.testing = True
    
        @patch('src.api.get_db')
        @patch('src.api.load_data')
        @patch('src.api.transform__df')
        @patch('src.api.df_to_list_of_dicts')
        def test_with_sample_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
            #mocking the database functions
            mock_session = MagicMock()
            mock_get_db.return_value.__enter__.return_value = mock_session
    
            # Mocking database query results
            mock_session.query.return_value.filter.return_value.all.return_value = []
            
            #Mocking the functions that are called in the api
            mock_load_data.return_value = mock_session
            mock_transform__df.return_value = mock_session
            mock_df_to_list_of_dicts.return_value = [
                {'id': '1', 'customer_id': 'customer1', 'customer_unique_id': 'unique1', 'customer_zip_code_prefix': '12345', 'customer_city': 'City1', 'customer_state': 'State1'},
                {'id': '2', 'customer_id': 'customer2', 'customer_unique_id': 'unique2', 'customer_zip_code_prefix': '67890', 'customer_city': 'City2', 'customer_state': 'State2'}
            ]  
    
            mock_file = (BytesIO(b'mock,file,content'), 'customers.csv')
    
            response = self.app.post('/api/load_customers_data',
                                        content_type='multipart/form-data',
                                        data={'file': mock_file})
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['status'], 'success')
            self.assertEqual(data['message'], 'Data processed successfully')
            self.assertEqual(len(data['body']), 2)
            self.assertEqual(data['new_customers_count'], 2)
            self.assertEqual(data['existing_customers_count'], 0)
    
        def test_api_load_customers_data_no_file(self):
            response = self.app.post('/api/load_customers_data')
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['error'], 'No file part')
    
        @patch('src.api.get_db')
        @patch('src.api.load_data')
        @patch('src.api.transform__df')
        @patch('src.api.df_to_list_of_dicts')
        def test_with_invalid_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
            mock_df_to_list_of_dicts.return_value = [{'invalid_column': 'data'}]
            mock_file = (BytesIO(b'mock,file,content'), 'invalid_customers.csv')
            response = self.app.post('/api/load_customers_data',
                                        content_type='multipart/form-data',
                                        data={'file': mock_file})
            
            self.assertEqual(response.status_code, 400)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['error'], 'Invalid file, Kindly provide the Customers csv file')
        
        @patch('src.api.get_db')
        def test_with_database_error(self, mock_get_db):
            mock_session = MagicMock()
            mock_get_db.return_value.__enter__.return_value = mock_session
            mock_session.query.side_effect = Exception('An error occurred in the Database')

            mock_file = (BytesIO(b'mock,file,content'), 'customers.csv')
            response = self.app.post('/api/load_customers_data',
                                        content_type='multipart/form-data',
                                        data={'file': mock_file})

            self.assertEqual(response.status_code, 500)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['status'], 'error')


class TestGetCustomers(unittest.TestCase):
        
            def setUp(self):
                self.app = app.test_client()
                self.app.testing = True
        
            @patch('src.api.get_db')
            def test_with_valid_data(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                mock_customers = [
                    Customers(id=1, customer_id='customer1', customer_unique_id='unique1', customer_zip_code_prefix='12345', customer_city='City1', customer_state='State1'),
                    Customers(id=2, customer_id='customer2', customer_unique_id='unique2', customer_zip_code_prefix='67890', customer_city='City2', customer_state='State2')
                ]
                mock_session.query.return_value.all.return_value = mock_customers
        
                response = self.app.get('/api/get_customers')
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'success')
                self.assertEqual(data['message'], "Customers data retrieved successfully")
                self.assertEqual(len(data['body']), 2)
                self.assertEqual(data['body'][0]['customer_id'], 'customer1')
                self.assertEqual(data['body'][1]['customer_id'], 'customer2')
        
            @patch('src.api.get_db')
            def test_get_customers_empty(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                mock_session.query.return_value.all.return_value = []
        
                response = self.app.get('/api/get_customers')
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'success')
                self.assertEqual(data['message'], "No Customers data available")
                self.assertEqual(len(data['body']), 0)
        
            @patch('src.api.get_db')
            def test_get_customers_error(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                mock_session.query.side_effect = Exception('An error occurred')
        
                response = self.app.get('/api/get_customers')
                self.assertEqual(response.status_code, 500)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'error')

class TestLoadOrders(unittest.TestCase):
    
        def setUp(self):
            self.app = app.test_client()
            self.app.testing = True
    
        @patch('src.api.get_db')
        @patch('src.api.load_data')
        @patch('src.api.transform__df')
        @patch('src.api.df_to_list_of_dicts')
        def test_with_sample_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
            #mocking the database functions
            mock_session = MagicMock()
            mock_get_db.return_value.__enter__.return_value = mock_session
    
            # Mocking database query results
            mock_session.query.return_value.filter.return_value.all.return_value = []
            
            #Mocking the functions that are called in the api
            mock_load_data.return_value = mock_session
            mock_transform__df.return_value = mock_session
            mock_df_to_list_of_dicts.return_value = [
                {'id': '1', 'order_id': 'order1', 'customer_id': 'customer1', 'order_status': 'delivered', 'order_purchase_timestamp': '2021-01-01 00:00:00', 'order_approved_at': '2021-01-01 00:00:00', 'order_delivered_carrier_date': '2021-01-01 00:00:00', 'order_delivered_customer_date': '2021-01-01 00:00:00', 'order_estimated_delivery_date': '2021-01-01 00:00:00'},
                {'id': '2', 'order_id': 'order2', 'customer_id': 'customer2', 'order_status': 'shipped', 'order_purchase_timestamp': '2021-01-01 00:00:00', 'order_approved_at': '2021-01-01 00:00:00', 'order_delivered_carrier_date': '2021-01-01 00:00:00', 'order_delivered_customer_date': '2021-01-01 00:00:00', 'order_estimated_delivery_date': '2021-01-01 00:00:00'}
            ]  
    
            mock_file = (BytesIO(b'mock,file,content'), 'orders.csv')
    
            response = self.app.post('/api/load_orders_data',
                                        content_type='multipart/form-data',
                                        data={'file': mock_file})
            
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['status'], 'success')
            self.assertEqual(data['message'], 'Data processed successfully')
            self.assertEqual(len(data['body']), 2)
            self.assertEqual(data['new_orders_count'], 2)
            self.assertEqual(data['existing_orders_count'], 0)

        def test_api_load_orders_data_no_file(self):
            response = self.app.post('/api/load_orders_data')
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['error'], 'No file part')

        @patch('src.api.get_db')
        @patch('src.api.load_data')
        @patch('src.api.transform__df')
        @patch('src.api.df_to_list_of_dicts')
        def test_with_invalid_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
            mock_df_to_list_of_dicts.return_value = [{'invalid_column': 'data'}]
            mock_file = (BytesIO(b'mock,file,content'), 'invalid_orders.csv')
            response = self.app.post('/api/load_orders_data',
                                        content_type='multipart/form-data',
                                        data={'file': mock_file})
            
            self.assertEqual(response.status_code, 400)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['error'], 'Invalid file, Kindly provide the Orders csv file')

        @patch('src.api.get_db')
        def test_with_database_error(self, mock_get_db):
            mock_session = MagicMock()
            mock_get_db.return_value.__enter__.return_value = mock_session
            mock_session.query.side_effect = Exception('An error occurred in the Database')

            mock_file = (BytesIO(b'mock,file,content'), 'orders.csv')
            response = self.app.post('/api/load_orders_data',
                                        content_type='multipart/form-data',
                                        data={'file': mock_file})

            self.assertEqual(response.status_code, 500)
            data = json.loads(response.data.decode('utf-8'))
            self.assertEqual(data['status'], 'error')


class TestGetOrders(unittest.TestCase):
            
            def setUp(self):
                self.app = app.test_client()
                self.app.testing = True
        
            @patch('src.api.get_db')
            def test_with_valid_data(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                mock_orders = [
                    Orders(id=1, order_id='order1', customer_id='customer1', order_status='delivered', order_purchase_timestamp='2021-01-01 00:00:00', order_approved_at='2021-01-01 00:00:00', order_delivered_carrier_date='2021-01-01 00:00:00', order_delivered_customer_date='2021-01-01 00:00:00', order_estimated_delivery_date='2021-01-01 00:00:00'),
                    Orders(id=2, order_id='order2', customer_id='customer2', order_status='shipped', order_purchase_timestamp='2021-01-01 00:00:00', order_approved_at='2021-01-01 00:00:00', order_delivered_carrier_date='2021-01-01 00:00:00', order_delivered_customer_date='2021-01-01 00:00:00', order_estimated_delivery_date='2021-01-01 00:00:00')
                ]
                mock_session.query.return_value.all.return_value = mock_orders
        
                response = self.app.get('/api/get_orders')
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'success')
                self.assertEqual(data['message'], "Orders data retrieved successfully")
                self.assertEqual(len(data['body']), 2)
                self.assertEqual(data['body'][0]['order_id'], 'order1')
                self.assertEqual(data['body'][1]['order_id'], 'order2')
        
            @patch('src.api.get_db')
            def test_get_orders_empty(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                mock_session.query.return_value.all.return_value = []
        
                response = self.app.get('/api/get_orders')
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
            
                self.assertEqual(data['status'], 'success')
                self.assertEqual(data['message'], "No Orders data available")
                self.assertEqual(len(data['body']), 0)
            
            @patch('src.api.get_db')
            def test_get_orders_error(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                mock_session.query.side_effect = Exception('An error occurred')
        
                response = self.app.get('/api/get_orders')
                self.assertEqual(response.status_code, 500)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'error')

class TestLoadOrderItems(unittest.TestCase):
        
            def setUp(self):
                self.app = app.test_client()
                self.app.testing = True
        
            @patch('src.api.get_db')
            @patch('src.api.load_data')
            @patch('src.api.transform__df')
            @patch('src.api.df_to_list_of_dicts')
            def test_with_sample_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
                #mocking the database functions
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                # Mocking database query results
                mock_session.query.return_value.filter.return_value.all.return_value = []
                
                #Mocking the functions that are called in the api
                mock_load_data.return_value = mock_session
                mock_transform__df.return_value = mock_session
                mock_df_to_list_of_dicts.return_value = [
                    {'id': '1', 'order_id': 'order1', 'order_item_id': 'item1', 'product_id': 'product1', 'seller_id': 'seller1', 'shipping_limit_date': '2021-01-01 00:00:00', 'price': 100.0, 'freight_value': 10.0},
                    {'id': '2', 'order_id': 'order2', 'order_item_id': 'item2', 'product_id': 'product2', 'seller_id': 'seller2', 'shipping_limit_date': '2021-01-01 00:00:00', 'price': 200.0, 'freight_value': 20.0}
                ]  
        
                mock_file = (BytesIO(b'mock,file,content'), 'order_items.csv')
        
                response = self.app.post('/api/load_order_items_data',
                                            content_type='multipart/form-data',
                                            data={'file': mock_file})
                
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'success')
                self.assertEqual(data['message'], 'Data processed successfully')
                self.assertEqual(len(data['body']), 2)
                self.assertEqual(data['new_order_items_count'], 2)
                self.assertEqual(data['existing_order_items_count'], 0)
    
            def test_api_load_order_items_data_no_file(self):
                response = self.app.post('/api/load_order_items_data')
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['error'], 'No file part')

            @patch('src.api.get_db')
            @patch('src.api.load_data')
            @patch('src.api.transform__df')
            @patch('src.api.df_to_list_of_dicts')
            def test_with_invalid_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
                mock_df_to_list_of_dicts.return_value = [{'invalid_column': 'data'}]
                mock_file = (BytesIO(b'mock,file,content'), 'invalid_order_items.csv')
                response = self.app.post('/api/load_order_items_data',
                                            content_type='multipart/form-data',
                                            data={'file': mock_file})
                
                self.assertEqual(response.status_code, 400)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['error'], 'Invalid file, Kindly provide the Order Items csv file')

            @patch('src.api.get_db')
            def test_with_database_error(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
                mock_session.query.side_effect = Exception('An error occurred in the Database')

                mock_file = (BytesIO(b'mock,file,content'), 'order_items.csv')
                response = self.app.post('/api/load_order_items_data',
                                            content_type='multipart/form-data',
                                            data={'file': mock_file})

                self.assertEqual(response.status_code, 500)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'error')

#  class TestGetOrderItems(unittest.TestCase):
                    
#                     def setUp(self):
#                         self.app = app.test_client()
#                         self.app.testing = True
                
#                     @patch('src.api.get_db')
#                     def test_with_valid_data(self, mock_get_db):
#                         mock_session = MagicMock()
#                         mock_get_db.return_value.__enter__.return_value = mock_session
                
#                         mock_order_items = [
#                             Order_Items(id=1, order_id='order1', order_item_id='item1', product_id='product1', seller_id='seller1', shipping_limit_date='2021-01-01 00:00:00', price=100.0, freight_value=10.0),
#                             Order_Items(id=2, order_id='order2', order_item_id='item2', product_id='product2', seller_id='seller2', shipping_limit_date='2021-01-01 00:00:00', price=200.0, freight_value=20.0)
#                         ]
#                         mock_session.query.return_value.all.return_value = mock_order_items
                
#                         response = self.app.get('/api/get_order_items')
#                         self.assertEqual(response.status_code, 200)
#                         data = json.loads(response.data.decode('utf-8'))
#                         self.assertEqual(data['status'], 'success')
#                         self.assertEqual(data['message'], "Order Items data retrieved successfully")
#                         self.assertEqual(len(data['body']), 2)
#                         self.assertEqual(data['body'][0]['order_id'], 'order1')
#                         self.assertEqual(data['body'][1]['order_id'], 'order2')
                
#                     @patch('src.api.get_db')
#                     def test_get_order_items_empty(self, mock_get_db):
#                         mock_session = MagicMock()
#                         mock_get_db.return_value.__enter__.return_value = mock_session
                
#                         mock_session.query.return_value.all.return_value = []
                
#                         response = self.app.get('/api/get_order_items')
#                         self.assertEqual(response.status_code, 200)
#                         data = json.loads(response.data.decode('utf-8'))
#                         self.assertEqual(data['status'], 'success')
#                         self.assertEqual(data['message'], "No Order Items data available")
#                         self.assertEqual(len(data['body']), 0)
                    
#                     @patch('src.api.get_db')
#                     def test_get_order_items_error(self, mock_get_db):
#                         mock_session = MagicMock()
#                         mock_get_db.return_value.__enter__.return_value = mock_session
                
#                         mock_session.query.side_effect = Exception('An error occurred')
                
#                         response = self.app.get('/api/get_order_items') 
#                         print("raw response", response)        
#                         self.assertEqual(response.status_code, 500) 
#                         data = json.loads(response.data.decode('utf-8'))
#                         self.assertEqual(data['status'], 'error')


class TestLoadOrderPayments(unittest.TestCase):
        
            def setUp(self):
                self.app = app.test_client()
                self.app.testing = True
        
            @patch('src.api.get_db')
            @patch('src.api.load_data')
            @patch('src.api.transform__df')
            @patch('src.api.df_to_list_of_dicts')
            def test_with_sample_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
                #mocking the database functions
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
        
                # Mocking database query results
                mock_session.query.return_value.filter.return_value.all.return_value = []
                
                #Mocking the functions that are called in the api
                mock_load_data.return_value = mock_session
                mock_transform__df.return_value = mock_session
                mock_df_to_list_of_dicts.return_value = [
                    {'id': '1', 'order_id': 'order1', 'payment_sequential': 1, 'payment_type': 'credit_card', 'payment_installments': 1, 'payment_value': 100.0},
                    {'id': '2', 'order_id': 'order2', 'payment_sequential': 2, 'payment_type': 'debit_card', 'payment_installments': 2, 'payment_value': 200.0}
                ]  
        
                mock_file = (BytesIO(b'mock,file,content'), 'order_payments.csv')
        
                response = self.app.post('/api/load_order_payments_data',
                                            content_type='multipart/form-data',
                                            data={'file': mock_file})
                
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'success')
                self.assertEqual(data['message'], 'Data processed successfully')
                self.assertEqual(len(data['body']), 2)
                self.assertEqual(data['new_order_payments_count'], 2)
                self.assertEqual(data['existing_order_payments_count'], 0)

            def test_api_load_order_payments_data_no_file(self):
                response = self.app.post('/api/load_order_payments_data')
                self.assertEqual(response.status_code, 200)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['error'], 'No file part')

            @patch('src.api.get_db')
            @patch('src.api.load_data')
            @patch('src.api.transform__df')
            @patch('src.api.df_to_list_of_dicts')
            def test_with_invalid_data(self, mock_df_to_list_of_dicts, mock_transform__df, mock_load_data, mock_get_db):
                mock_df_to_list_of_dicts.return_value = [{'invalid_column': 'data'}]
                mock_file = (BytesIO(b'mock,file,content'), 'invalid_order_payments.csv')
                response = self.app.post('/api/load_order_payments_data',
                                            content_type='multipart/form-data',
                                            data={'file': mock_file})
                
                self.assertEqual(response.status_code, 400)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['error'], 'Invalid file, Kindly provide the Order Payments csv file')

            @patch('src.api.get_db')
            def test_with_database_error(self, mock_get_db):
                mock_session = MagicMock()
                mock_get_db.return_value.__enter__.return_value = mock_session
                mock_session.query.side_effect = Exception('An error occurred in the Database')

                mock_file = (BytesIO(b'mock,file,content'), 'order_payments.csv')
                response = self.app.post('/api/load_order_payments_data',
                                            content_type='multipart/form-data',
                                            data={'file': mock_file})

                self.assertEqual(response.status_code, 500)
                data = json.loads(response.data.decode('utf-8'))
                self.assertEqual(data['status'], 'error')             

class TestGetOrderPayments(unittest.TestCase):
                        
                        def setUp(self):
                            self.app = app.test_client()
                            self.app.testing = True
                    
                        @patch('src.api.get_db')
                        def test_with_valid_data(self, mock_get_db):
                            mock_session = MagicMock()
                            mock_get_db.return_value.__enter__.return_value = mock_session
                    
                            mock_order_payments = [
                                Order_Payments(id=1, order_id='order1', payment_sequential=1, payment_type='credit_card', payment_installments=1, payment_value=100.0),
                                Order_Payments(id=2, order_id='order2', payment_sequential=2, payment_type='debit_card', payment_installments=2, payment_value=200.0)
                            ]
                            mock_session.query.return_value.all.return_value = mock_order_payments
                    
                            response = self.app.get('/api/get_order_payments')
                            self.assertEqual(response.status_code, 200)
                            data = json.loads(response.data.decode('utf-8'))
                            self.assertEqual(data['status'], 'success')
                            self.assertEqual(data['message'], "Order Payments data retrieved successfully")
                            self.assertEqual(len(data['body']), 2)
                            self.assertEqual(data['body'][0]['order_id'], 'order1')
                            self.assertEqual(data['body'][1]['order_id'], 'order2')
                    
                        @patch('src.api.get_db')
                        def test_get_order_payments_empty(self, mock_get_db):
                            mock_session = MagicMock()
                            mock_get_db.return_value.__enter__.return_value = mock_session
                    
                            mock_session.query.return_value.all.return_value = []
                    
                            response = self.app.get('/api/get_order_payments')
                            self.assertEqual(response.status_code, 200)
                            data = json.loads(response.data.decode('utf-8'))
                            self.assertEqual(data['status'], 'success')
                            self.assertEqual(data['message'], "No Order Payments data available")
                            self.assertEqual(len(data['body']), 0)
                        
                        @patch('src.api.get_db')
                        def test_get_order_payments_error(self, mock_get_db):
                            mock_session = MagicMock()
                            mock_get_db.return_value.__enter__.return_value = mock_session
                    
                            mock_session.query.side_effect = Exception('An error occurred')
                    
                            response = self.app.get('/api/get_order_payments')         
                            self.assertEqual(response.status_code, 500) 
                            data = json.loads(response.data.decode('utf-8'))
                            self.assertEqual(data['status'], 'error')   

if __name__ == '__main__':
    unittest.main()

                        
                     