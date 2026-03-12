"""
Trial and subscription status helpers
"""
import logging
from datetime import datetime
from bson import ObjectId

from utils.database import db

logger = logging.getLogger(__name__)


async def get_trial_status(company_id: str) -> dict:
    """Check trial/subscription status for a company"""
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    if not company:
        return {"status": "unknown", "is_active": False}
    
    subscription_status = company.get("subscription_status", "trialing")
    trial_end_str = company.get("trial_end")
    
    if subscription_status == "active":
        return {
            "status": "active",
            "is_active": True,
            "plan": company.get("subscription_plan", "pro"),
            "message": "Active subscription"
        }
    
    if trial_end_str:
        try:
            trial_end = datetime.fromisoformat(trial_end_str.replace('Z', '+00:00'))
            if isinstance(trial_end, datetime) and trial_end.tzinfo is None:
                trial_end = trial_end.replace(tzinfo=None)
            now = datetime.utcnow()
            
            days_left = (trial_end - now).days
            
            if days_left > 0:
                return {
                    "status": "trialing",
                    "is_active": True,
                    "days_left": days_left,
                    "trial_end": trial_end_str,
                    "message": f"Trial: {days_left} days remaining"
                }
            else:
                return {
                    "status": "trial_expired",
                    "is_active": False,
                    "days_left": 0,
                    "trial_end": trial_end_str,
                    "message": "Trial expired - Please upgrade to continue"
                }
        except Exception as e:
            logger.error(f"Error parsing trial_end: {e}")
    
    return {
        "status": "trial_expired", 
        "is_active": False,
        "message": "Trial expired - Please upgrade to continue"
    }


async def check_trial_active(company_id: str) -> bool:
    """Quick check if trial/subscription is active"""
    status = await get_trial_status(company_id)
    return status.get("is_active", False)
