from datetime import datetime
import logging
from typing import Dict, Optional

class PackageService:
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
    def validate_customer(self, phone_number: str) -> bool:
        """Validasi nomor pelanggan"""
        try:
            customer = self.db.query(
                "SELECT status FROM customers WHERE phone_number = %s",
                (phone_number,)
            )
            return customer and customer['status'] == 'ACTIVE'
        except Exception as e:
            self.logger.error(f"Error validating customer: {e}")
            return False
    
    def get_package_info(self, package_code: str) -> Optional[Dict]:
        """Mendapatkan informasi paket"""
        try:
            package = self.db.query(
                "SELECT * FROM packages WHERE code = %s AND status = 'ACTIVE'",
                (package_code,)
            )
            return package
        except Exception as e:
            self.logger.error(f"Error getting package info: {e}")
            return None
    
    def check_balance(self, phone_number: str) -> float:
        """Cek saldo pelanggan"""
        try:
            balance = self.db.query(
                "SELECT balance FROM customer_balance WHERE phone_number = %s",
                (phone_number,)
            )
            return float(balance['balance'])
        except Exception as e:
            self.logger.error(f"Error checking balance: {e}")
            return 0.0
    
    def deduct_balance(self, phone_number: str, amount: float) -> bool:
        """Kurangi saldo pelanggan"""
        try:
            self.db.execute(
                """
                UPDATE customer_balance 
                SET balance = balance - %s 
                WHERE phone_number = %s AND balance >= %s
                """,
                (amount, phone_number, amount)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error deducting balance: {e}")
            return False
    
    def activate_package(self, phone_number: str, package: Dict) -> bool:
        """Aktivasi paket data"""
        try:
            self.db.execute(
                """
                INSERT INTO active_packages 
                (phone_number, package_code, quota, validity_period, activated_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    phone_number,
                    package['code'],
                    package['quota'],
                    package['validity_period'],
                    datetime.now()
                )
            )
            return True
        except Exception as e:
            self.logger.error(f"Error activating package: {e}")
            return False
    
    def purchase_package(self, phone_number: str, package_code: str) -> Dict:
        """Proses utama pembelian paket"""
        result = {
            'status': 'failed',
            'message': 'Unknown error occurred'
        }
        
        # Validasi pelanggan
        if not self.validate_customer(phone_number):
            result['message'] = 'Invalid phone number'
            return result
        
        # Get package info
        package = self.get_package_info(package_code)
        if not package:
            result['message'] = 'Package not available'
            return result
        
        # Check balance
        balance = self.check_balance(phone_number)
        if balance < package['price']:
            result['message'] = 'Insufficient balance'
            return result
        
        # Process payment
        if not self.deduct_balance(phone_number, package['price']):
            result['message'] = 'Payment processing failed'
            return result
        
        # Activate package
        if self.activate_package(phone_number, package):
            result['status'] = 'success'
            result['message'] = 'Package activated successfully'
            result['remaining_balance'] = balance - package['price']
            result['package_details'] = package
        else:
            # Rollback payment if activation fails
            self.db.execute(
                "UPDATE customer_balance SET balance = balance + %s WHERE phone_number = %s",
                (package['price'], phone_number)
            )
            result['message'] = 'Package activation failed'
        
        return result
