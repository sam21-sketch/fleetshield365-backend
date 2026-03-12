"""
Alert system for expiry warnings and issue tracking
"""
import logging
from datetime import datetime
from bson import ObjectId
from typing import List

from utils.database import db
from models.schemas import UserRole
from services.email_service import email_service

logger = logging.getLogger(__name__)


async def check_and_create_expiry_alerts(vehicle: dict, company_id: str):
    """Check vehicle expiry dates and create alerts at 60, 30, 14, 7 day intervals"""
    now = datetime.utcnow()
    vehicle_name = f"{vehicle['name']} ({vehicle['registration_number']})"
    vehicle_id = str(vehicle['_id'])
    
    REMINDER_DAYS = [60, 30, 14, 7]
    
    expiry_fields = [
        ('rego_expiry', 'Registration'),
        ('insurance_expiry', 'Insurance'),
        ('safety_certificate_expiry', 'Safety Certificate'),
        ('coi_expiry', 'COI (Certificate of Inspection)')
    ]
    
    for field, label in expiry_fields:
        expiry_date_str = vehicle.get(field)
        if expiry_date_str:
            try:
                expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                days_until_expiry = (expiry_date - now).days
                
                if days_until_expiry < 0:
                    existing_alert = await db.alerts.find_one({
                        "vehicle_id": vehicle_id,
                        "type": "expiry_critical",
                        "message": {"$regex": f"{label}.*EXPIRED"}
                    })
                    
                    if not existing_alert:
                        message = f"{label} for {vehicle_name} has EXPIRED! (was due {expiry_date_str})"
                        await create_alert(company_id, "expiry_critical", message, vehicle_id)
                
                else:
                    for reminder_day in REMINDER_DAYS:
                        if days_until_expiry <= reminder_day:
                            if days_until_expiry <= 7:
                                alert_type = "expiry_critical"
                                urgency = "CRITICAL"
                            elif days_until_expiry <= 14:
                                alert_type = "expiry_warning"
                                urgency = "URGENT"
                            elif days_until_expiry <= 30:
                                alert_type = "expiry_warning"
                                urgency = "ACTION NEEDED"
                            else:
                                alert_type = "expiry_warning"
                                urgency = "HEADS UP"
                            
                            existing_alert = await db.alerts.find_one({
                                "vehicle_id": vehicle_id,
                                "type": alert_type,
                                "message": {"$regex": f"{label}.*{vehicle_name}.*{reminder_day}"}
                            })
                            
                            if not existing_alert:
                                message = f"[{urgency}] {label} for {vehicle_name} expires in {days_until_expiry} days ({expiry_date_str})"
                                await create_alert(company_id, alert_type, message, vehicle_id)
                            
                            break
                            
            except ValueError:
                pass


async def create_alert(company_id: str, alert_type: str, message: str, vehicle_id: str = None, driver_id: str = None):
    """Create alert and send notification"""
    alert = {
        "_id": ObjectId(),
        "company_id": company_id,
        "type": alert_type,
        "message": message,
        "vehicle_id": vehicle_id,
        "driver_id": driver_id,
        "is_read": False,
        "email_sent": False,
        "created_at": datetime.utcnow()
    }
    await db.alerts.insert_one(alert)
    
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": [UserRole.SUPER_ADMIN, UserRole.ADMIN]}
    }).to_list(100)
    
    admin_emails = [admin['email'] for admin in admins if admin.get('email')]
    
    if admin_emails:
        await email_service.send_alert_email(alert_type, message, admin_emails, company_id)
        await db.alerts.update_one({"_id": alert["_id"]}, {"$set": {"email_sent": True}})
    
    return alert


async def log_audit_trail(user_id: str, action: str, entity_type: str, entity_id: str, ip_address: str, changes: dict = None):
    """Log audit trail entry"""
    await db.audit_trail.insert_one({
        "user_id": user_id,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "timestamp": datetime.utcnow(),
        "ip_address": ip_address,
        "changes": changes or {}
    })
