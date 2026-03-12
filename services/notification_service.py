"""
Push notification service using Expo
"""
import httpx
import logging
from typing import List
from bson import ObjectId

from utils.database import db
from services.email_service import send_issue_alert_email

logger = logging.getLogger(__name__)


async def send_push_notification(push_tokens: List[str], title: str, body: str, data: dict = None):
    """Send push notification via Expo Push Notification service"""
    if not push_tokens:
        return
    
    messages = []
    for token in push_tokens:
        if token and token.startswith('ExponentPushToken'):
            messages.append({
                "to": token,
                "sound": "default",
                "title": title,
                "body": body,
                "data": data or {}
            })
    
    if not messages:
        return
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://exp.host/--/api/v2/push/send",
                json=messages,
                headers={"Content-Type": "application/json"}
            )
            logger.info(f"Push notifications sent: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send push notification: {e}")


async def notify_admins(company_id: str, notification_type: str, title: str, body: str, data: dict = None, email_func=None, email_args: tuple = None):
    """Send notifications to all admins of a company based on their preferences"""
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}
    }).to_list(100)
    
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "Your Company") if company else "Your Company"
    
    for admin in admins:
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True, "expiry_alerts": True, "issue_alerts": True, "missed_inspection_alerts": True, "daily_summary": True}
        
        type_enabled = prefs.get(f"{notification_type}_alerts", True) if notification_type != "daily_summary" else prefs.get("daily_summary", False)
        
        if not type_enabled:
            continue
        
        if prefs.get("push_enabled", True):
            tokens = await db.push_tokens.find({"user_id": str(admin["_id"])}).to_list(10)
            push_tokens = [t["token"] for t in tokens if t.get("token")]
            await send_push_notification(push_tokens, title, body, data)
        
        if prefs.get("email_enabled", True) and email_func and email_args:
            await email_func(admin.get("email"), company_name, *email_args)


async def notify_admins_with_photos(company_id: str, vehicle_name: str, driver_name: str, issue_summary: str, inspection_type: str, photos: List[dict], inspection_id: str):
    """Send issue alert notifications to admins with photos included"""
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}
    }).to_list(100)
    
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "Your Company") if company else "Your Company"
    
    for admin in admins:
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True, "issue_alerts": True}
        
        if not prefs.get("issue_alerts", True):
            continue
        
        if prefs.get("push_enabled", True):
            tokens = await db.push_tokens.find({"user_id": str(admin["_id"])}).to_list(10)
            push_tokens = [t["token"] for t in tokens if t.get("token")]
            await send_push_notification(
                push_tokens, 
                f"DEFECT: {vehicle_name}", 
                f"Driver reported: {issue_summary}",
                {"inspection_id": inspection_id, "type": "defect_alert"}
            )
        
        if prefs.get("email_enabled", True) and admin.get("email"):
            await send_issue_alert_email(
                admin.get("email"),
                company_name,
                vehicle_name,
                driver_name,
                issue_summary,
                inspection_type,
                photos,
                inspection_id
            )
