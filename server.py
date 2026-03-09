from fastapi import FastAPI, APIRouter, HTTPException, Depends, status, Request, UploadFile, File, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from enum import Enum
import uuid
from datetime import datetime, timedelta, timezone
import bcrypt
import jwt
from bson import ObjectId
import base64
from io import BytesIO
import zipfile
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
from reportlab.lib.units import inch, cm
import json
import stripe
import httpx
import asyncio
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

# Stripe Configuration
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY', '')

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ.get('DB_NAME', 'fleetguard_db')]

# JWT Configuration
SECRET_KEY = os.environ.get('JWT_SECRET', 'fleetguard-secret-key-2025')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

# Create the main app
app = FastAPI(title="FleetShield365 API")
api_router = APIRouter(prefix="/api")
security = HTTPBearer()

# Simple in-memory cache for dashboard stats (reduces DB queries)
dashboard_cache: Dict[str, Any] = {}
CACHE_TTL_SECONDS = 30  # Cache for 30 seconds

def get_cached_stats(company_id: str) -> Optional[dict]:
    """Get cached dashboard stats if still valid"""
    cache_key = f"dashboard_{company_id}"
    if cache_key in dashboard_cache:
        cached = dashboard_cache[cache_key]
        if datetime.utcnow().timestamp() - cached["timestamp"] < CACHE_TTL_SECONDS:
            return cached["data"]
    return None

def set_cached_stats(company_id: str, data: dict):
    """Cache dashboard stats"""
    cache_key = f"dashboard_{company_id}"
    dashboard_cache[cache_key] = {
        "timestamp": datetime.utcnow().timestamp(),
        "data": data
    }

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SendGrid Configuration
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'noreply@fleetguard.app')

# ============== Email Notification Service ==============

async def send_email_notification(to_email: str, subject: str, html_content: str):
    """Send email notification via SendGrid"""
    if not SENDGRID_API_KEY:
        logger.warning("SendGrid API key not configured, skipping email")
        return False
    
    try:
        message = Mail(
            from_email=Email(SENDER_EMAIL, "FleetShield365 Alerts"),
            to_emails=To(to_email),
            subject=subject,
            html_content=Content("text/html", html_content)
        )
        
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        status_code = response.status_code
        logger.info(f"[SENDGRID-TEST] Email sent to {to_email}: {subject} (HTTP {status_code})")
        # SendGrid returns 200, 201, or 202 for success
        is_success = status_code in [200, 201, 202]
        logger.info(f"[SENDGRID-TEST] Return value: {is_success}")
        return is_success
    except Exception as e:
        logger.error(f"[SENDGRID] Error sending email: {e}")
        return False

async def send_expiry_alert_email(admin_email: str, company_name: str, alerts: List[dict]):
    """Send expiry alert email to admin"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Expiry Alerts</h2>
        <p>Hi {company_name} Admin,</p>
        <p>The following items require your attention:</p>
        <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
            <tr style="background-color: #1E293B; color: white;">
                <th style="padding: 12px; text-align: left;">Item</th>
                <th style="padding: 12px; text-align: left;">Type</th>
                <th style="padding: 12px; text-align: left;">Expiry Date</th>
                <th style="padding: 12px; text-align: left;">Status</th>
            </tr>
            {''.join([f'''
            <tr style="border-bottom: 1px solid #E2E8F0;">
                <td style="padding: 12px;">{alert.get('item_name', 'N/A')}</td>
                <td style="padding: 12px;">{alert.get('alert_type', 'N/A')}</td>
                <td style="padding: 12px;">{alert.get('expiry_date', 'N/A')}</td>
                <td style="padding: 12px; color: {'#DC2626' if alert.get('is_expired') else '#F97316'};">
                    {'EXPIRED' if alert.get('is_expired') else 'Expiring Soon'}
                </td>
            </tr>
            ''' for alert in alerts])}
        </table>
        <p>Please log in to FleetShield365 to take action.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] {len(alerts)} Expiry Alert(s) Require Attention", html_content)

async def send_issue_alert_email(admin_email: str, company_name: str, vehicle_name: str, driver_name: str, issue_summary: str, inspection_type: str):
    """Send issue alert email when an inspection has issues"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #DC2626;">FleetShield365 Issue Alert</h2>
        <p>Hi {company_name} Admin,</p>
        <p>An issue has been reported during a vehicle inspection:</p>
        <div style="background-color: #FEF2F2; border: 1px solid #FECACA; padding: 16px; border-radius: 8px; margin: 20px 0;">
            <p><strong>Vehicle:</strong> {vehicle_name}</p>
            <p><strong>Driver:</strong> {driver_name}</p>
            <p><strong>Inspection Type:</strong> {inspection_type}</p>
            <p><strong>Issue Summary:</strong></p>
            <p style="color: #DC2626;">{issue_summary}</p>
        </div>
        <p>Please log in to FleetShield365 to review the full inspection report.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] Issue Reported: {vehicle_name}", html_content)

async def send_missed_inspection_email(admin_email: str, company_name: str, vehicles: List[dict]):
    """Send missed inspection alert email"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Missed Inspection Alert</h2>
        <p>Hi {company_name} Admin,</p>
        <p>The following vehicles did not complete their prestart inspection today:</p>
        <ul style="margin: 20px 0;">
            {''.join([f'<li style="padding: 8px 0;">{v.get("name", "Unknown")} ({v.get("registration_number", "N/A")})</li>' for v in vehicles])}
        </ul>
        <p>Please follow up with the assigned drivers.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] {len(vehicles)} Vehicle(s) Missed Inspection Today", html_content)

async def send_daily_summary_email(admin_email: str, company_name: str, summary: dict):
    """Send daily summary email"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Daily Summary</h2>
        <p>Hi {company_name} Admin,</p>
        <p>Here's your fleet summary for today:</p>
        <div style="background-color: #F8FAFC; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <p><strong>Inspections Completed:</strong> {summary.get('completed', 0)}</p>
            <p><strong>Inspections Missed:</strong> {summary.get('missed', 0)}</p>
            <p><strong>Issues Reported:</strong> {summary.get('issues', 0)}</p>
            <p><strong>Fuel Submissions:</strong> {summary.get('fuel_submissions', 0)}</p>
            <p><strong>Total Fuel (L):</strong> {summary.get('total_fuel', 0):.1f}</p>
        </div>
        <p>Log in to FleetGuard for detailed reports.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] Daily Summary - {datetime.now().strftime('%B %d, %Y')}", html_content)

# ============== Push Notification Service ==============

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
    # Get all admins for this company
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}
    }).to_list(100)
    
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "Your Company") if company else "Your Company"
    
    for admin in admins:
        # Get notification preferences
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True, "expiry_alerts": True, "issue_alerts": True, "missed_inspection_alerts": True, "daily_summary": True}
        
        # Check if this notification type is enabled
        type_enabled = prefs.get(f"{notification_type}_alerts", True) if notification_type != "daily_summary" else prefs.get("daily_summary", False)
        
        if not type_enabled:
            continue
        
        # Send push notification
        if prefs.get("push_enabled", True):
            tokens = await db.push_tokens.find({"user_id": str(admin["_id"])}).to_list(10)
            push_tokens = [t["token"] for t in tokens if t.get("token")]
            await send_push_notification(push_tokens, title, body, data)
        
        # Send email notification
        if prefs.get("email_enabled", True) and email_func and email_args:
            await email_func(admin.get("email"), company_name, *email_args)

# ============== Helper Functions ==============

def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

import re
async def generate_unique_username(name: str, company_id: str) -> str:
    """Generate a unique username from the person's name"""
    # Clean the name: lowercase, remove special chars, replace spaces with dots
    base_username = re.sub(r'[^a-z0-9]', '.', name.lower().strip())
    base_username = re.sub(r'\.+', '.', base_username).strip('.')  # Clean up multiple dots
    
    if not base_username:
        base_username = "user"
    
    # Check if username exists in the same company
    username = base_username
    counter = 1
    while await db.users.find_one({"username": username, "company_id": company_id}):
        username = f"{base_username}{counter}"
        counter += 1
    
    return username

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        user['id'] = str(user['_id'])
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable dict"""
    if doc is None:
        return None
    if isinstance(doc, list):
        return [serialize_doc(d) for d in doc]
    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            if key == '_id':
                result['id'] = str(value)
            elif isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, list):
                result[key] = serialize_doc(value)
            elif isinstance(value, dict):
                result[key] = serialize_doc(value)
            else:
                result[key] = value
        return result
    return doc

# ============== Pydantic Models ==============

class UserRole:
    SUPER_ADMIN = "super_admin"
    ADMIN = "admin"
    DRIVER = "driver"

class VehicleStatus:
    ACTIVE = "active"
    UNDER_MAINTENANCE = "under_maintenance"
    REGO_EXPIRED = "rego_expired"
    SAFETY_INSPECTION_DUE = "safety_inspection_due"

class InspectionType:
    PRESTART = "prestart"
    END_SHIFT = "end_shift"

class ChecklistItemStatus:
    OK = "ok"
    ISSUE = "issue"
    NOT_APPLICABLE = "not_applicable"

class AIDamageStatus:
    NO_DAMAGE = "no_damage"
    POSSIBLE_DAMAGE = "possible_damage"
    CONFIRMED_DAMAGE = "confirmed_damage"

# Auth Models
class UserRegister(BaseModel):
    email: Optional[EmailStr] = None  # Optional - can login with username instead
    password: str
    name: str
    username: Optional[str] = None  # Auto-generated if not provided
    phone: Optional[str] = None
    role: str = UserRole.DRIVER
    company_id: Optional[str] = None
    # Driver license and training details
    license_number: Optional[str] = None
    license_class: Optional[str] = None
    license_expiry: Optional[str] = None  # YYYY-MM-DD or "NA"
    medical_certificate_expiry: Optional[str] = None
    first_aid_expiry: Optional[str] = None
    forklift_license_expiry: Optional[str] = None
    dangerous_goods_expiry: Optional[str] = None

class DriverUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    license_number: Optional[str] = None
    license_class: Optional[str] = None
    license_expiry: Optional[str] = None
    medical_certificate_expiry: Optional[str] = None
    first_aid_expiry: Optional[str] = None
    forklift_license_expiry: Optional[str] = None
    dangerous_goods_expiry: Optional[str] = None

class UserLogin(BaseModel):
    email: Optional[str] = None  # Can be email or username
    username: Optional[str] = None  # Alternative to email
    password: str
    remember_me: bool = False  # Keep logged in option

# Fuel Submission Models
class FuelSubmission(BaseModel):
    vehicle_id: str
    amount: float  # Dollar amount
    liters: float
    receipt_photo_base64: Optional[str] = None
    odometer: Optional[int] = None
    fuel_station: Optional[str] = None
    notes: Optional[str] = None

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict

# Company Models
class CompanyCreate(BaseModel):
    name: str
    logo_base64: Optional[str] = None

class CompanyUpdate(BaseModel):
    name: Optional[str] = None
    logo_base64: Optional[str] = None
    subscription_plan: Optional[str] = None

# Vehicle Models
class VehicleCreate(BaseModel):
    name: str
    registration_number: str
    trailer_attached: Optional[str] = None
    status: str = VehicleStatus.ACTIVE
    rego_expiry: Optional[str] = None
    insurance_expiry: Optional[str] = None
    safety_certificate_expiry: Optional[str] = None
    coi_expiry: Optional[str] = None
    service_due_km: Optional[int] = None
    current_odometer: Optional[int] = 0

class VehicleUpdate(BaseModel):
    name: Optional[str] = None
    registration_number: Optional[str] = None
    trailer_attached: Optional[str] = None
    status: Optional[str] = None
    rego_expiry: Optional[str] = None
    insurance_expiry: Optional[str] = None
    safety_certificate_expiry: Optional[str] = None
    coi_expiry: Optional[str] = None
    service_due_km: Optional[int] = None
    current_odometer: Optional[int] = None
    assigned_driver_ids: Optional[List[str]] = None

# Checklist Models
class ChecklistItem(BaseModel):
    name: str
    section: str
    status: str = ChecklistItemStatus.OK
    comment: Optional[str] = None

class InspectionPhoto(BaseModel):
    photo_type: str  # front, rear, left, right, cabin, odometer, damage
    base64_data: str
    timestamp: str
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    ai_damage_status: str = AIDamageStatus.NO_DAMAGE

# Inspection Models
class PrestartCreate(BaseModel):
    vehicle_id: str
    odometer: int
    checklist_items: List[ChecklistItem]
    photos: List[InspectionPhoto]
    signature_base64: str
    declaration_confirmed: bool = True
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None

class EndShiftCreate(BaseModel):
    vehicle_id: str
    odometer: int
    fuel_level: str
    new_damage: bool = False
    incident_today: bool = False
    cleanliness: str  # clean, average, dirty
    damage_comment: Optional[str] = None
    incident_comment: Optional[str] = None
    photos: Optional[List[InspectionPhoto]] = []
    signature_base64: str
    declaration_confirmed: bool = True
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None

# Maintenance Models
class MaintenanceLogCreate(BaseModel):
    vehicle_id: str
    service_date: str
    service_type: str
    cost: float
    workshop_name: str
    invoice_base64: Optional[str] = None
    notes: Optional[str] = None

# Alert Models
class AlertCreate(BaseModel):
    type: str  # unsafe_vehicle, repeated_issues, expiry_warning, vehicle_offline
    message: str
    vehicle_id: Optional[str] = None
    driver_id: Optional[str] = None



# ============== Support Request Models ==============

class SupportRequestCategory(str, Enum):
    GENERAL = "general"
    TECHNICAL = "technical"
    BILLING = "billing"
    FEATURE_REQUEST = "feature_request"
    BUG_REPORT = "bug_report"

class SupportRequestStatus(str, Enum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    CLOSED = "closed"

class SupportRequestCreate(BaseModel):
    subject: str
    message: str
    category: SupportRequestCategory = SupportRequestCategory.GENERAL

class SupportRequestUpdate(BaseModel):
    status: Optional[SupportRequestStatus] = None
    admin_response: Optional[str] = None


# ============== Incident Report Models ==============

class IncidentSeverity:
    MINOR = "minor"
    MODERATE = "moderate"
    SEVERE = "severe"

class OtherPartyDetails(BaseModel):
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    vehicle_rego: Optional[str] = None
    insurance_company: Optional[str] = None
    insurance_policy: Optional[str] = None

class WitnessDetails(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    statement: Optional[str] = None

class IncidentCreate(BaseModel):
    vehicle_id: str
    description: str
    severity: str = IncidentSeverity.MODERATE  # minor, moderate, severe
    location_address: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    other_party: OtherPartyDetails
    witnesses: Optional[List[WitnessDetails]] = []
    police_report_number: Optional[str] = None
    injuries_occurred: bool = False
    injury_description: Optional[str] = None
    damage_photos: List[str] = []  # Base64 encoded photos
    other_vehicle_photos: List[str] = []  # Base64 encoded photos
    scene_photos: List[str] = []  # Base64 encoded photos

class IncidentUpdate(BaseModel):
    status: Optional[str] = None  # reported, under_review, resolved, closed
    admin_notes: Optional[str] = None
    insurance_claim_number: Optional[str] = None
    resolution_details: Optional[str] = None

# Driver Assignment
class DriverAssignment(BaseModel):
    driver_ids: List[str]

# Company Registration Model
class CompanyRegister(BaseModel):
    company_name: str
    name: str
    email: EmailStr
    password: str
    vehicle_count: int = 5
    origin_url: Optional[str] = None
    role: Optional[str] = None  # 'super_admin' for Company Owner, 'admin' for Admin

# Pricing Configuration
PRICING = {
    "base_price": 39,
    "per_vehicle": 5,
    "trial_days": 14,
}

# ============== PDF Generation ==============

async def generate_inspection_pdf(inspection: dict, vehicle: dict, driver: dict, company: dict) -> str:
    """Generate PDF report for inspection and return base64 encoded string"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    styles = getSampleStyleSheet()
    
    # Title Style
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#1a365d'), spaceAfter=12)
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#2d3748'), spaceAfter=8)
    normal_style = ParagraphStyle('Normal', parent=styles['Normal'], fontSize=10, spaceAfter=6)
    
    # Company Logo (if exists)
    if company and company.get('logo_base64'):
        try:
            logo_data = base64.b64decode(company['logo_base64'].split(',')[-1] if ',' in company['logo_base64'] else company['logo_base64'])
            logo_img = RLImage(BytesIO(logo_data), width=2*inch, height=1*inch)
            elements.append(logo_img)
            elements.append(Spacer(1, 12))
        except:
            pass
    
    # Title
    inspection_type = "Prestart Inspection Report" if inspection['type'] == 'prestart' else "End of Shift Report"
    elements.append(Paragraph(inspection_type, title_style))
    elements.append(Spacer(1, 12))
    
    # Basic Info Table
    info_data = [
        ['Date/Time:', inspection.get('timestamp', 'N/A')],
        ['Vehicle:', f"{vehicle.get('name', 'N/A')} ({vehicle.get('registration_number', 'N/A')})"],
        ['Driver:', driver.get('name', 'N/A')],
        ['Odometer:', f"{inspection.get('odometer', 'N/A')} km"],
    ]
    
    if inspection.get('gps_latitude') and inspection.get('gps_longitude'):
        info_data.append(['GPS Location:', f"{inspection['gps_latitude']:.6f}, {inspection['gps_longitude']:.6f}"])
    
    info_table = Table(info_data, colWidths=[100, 350])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e2e8f0')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#2d3748')),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cbd5e0')),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))
    
    # Checklist (for prestart)
    if inspection['type'] == 'prestart' and inspection.get('checklist_items'):
        elements.append(Paragraph("Inspection Checklist", heading_style))
        
        checklist_data = [['Item', 'Section', 'Status', 'Comment']]
        for item in inspection['checklist_items']:
            status_color = '✓' if item['status'] == 'ok' else ('⚠' if item['status'] == 'issue' else 'N/A')
            checklist_data.append([
                item['name'],
                item['section'],
                status_color,
                item.get('comment', '')[:50] if item.get('comment') else ''
            ])
        
        checklist_table = Table(checklist_data, colWidths=[150, 100, 60, 140])
        checklist_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2d3748')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cbd5e0')),
            ('ALIGN', (2, 0), (2, -1), 'CENTER'),
        ]))
        elements.append(checklist_table)
        elements.append(Spacer(1, 20))
    
    # End Shift specific info
    if inspection['type'] == 'end_shift':
        elements.append(Paragraph("End of Shift Details", heading_style))
        shift_data = [
            ['Fuel Level:', inspection.get('fuel_level', 'N/A')],
            ['Cleanliness:', inspection.get('cleanliness', 'N/A')],
            ['New Damage:', 'Yes' if inspection.get('new_damage') else 'No'],
            ['Incident Today:', 'Yes' if inspection.get('incident_today') else 'No'],
        ]
        if inspection.get('damage_comment'):
            shift_data.append(['Damage Comment:', inspection['damage_comment'][:100]])
        if inspection.get('incident_comment'):
            shift_data.append(['Incident Comment:', inspection['incident_comment'][:100]])
        
        shift_table = Table(shift_data, colWidths=[120, 330])
        shift_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e2e8f0')),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cbd5e0')),
        ]))
        elements.append(shift_table)
        elements.append(Spacer(1, 20))
    
    # Photos section (with actual images)
    if inspection.get('photos') and len(inspection['photos']) > 0:
        elements.append(Paragraph("Inspection Photos", heading_style))
        elements.append(Spacer(1, 10))
        
        # Add photos one by one (simpler approach)
        for i, photo in enumerate(inspection['photos'][:6]):  # Limit to 6 photos
            try:
                photo_base64 = photo.get('base64_data', '')
                if photo_base64:
                    # Remove data URL prefix if present
                    if ',' in photo_base64:
                        photo_base64 = photo_base64.split(',')[-1]
                    
                    photo_bytes = base64.b64decode(photo_base64)
                    photo_img = RLImage(BytesIO(photo_bytes), width=3*inch, height=2.5*inch)
                    
                    # Add photo type label
                    photo_type = photo.get('photo_type', 'Photo').replace('_', ' ').title()
                    elements.append(Paragraph(f"<b>{photo_type}</b>", normal_style))
                    elements.append(Spacer(1, 5))
                    elements.append(photo_img)
                    elements.append(Spacer(1, 15))
            except Exception as e:
                logger.error(f"Failed to add photo to PDF: {e}")
                continue
        
        if not any(p.get('base64_data') for p in inspection['photos'][:6]):
            elements.append(Paragraph("Photos on file (unable to render)", normal_style))
        
        elements.append(Spacer(1, 20))
    
    # Signature
    if inspection.get('signature_base64'):
        elements.append(Paragraph("Driver Signature", heading_style))
        try:
            sig_data = inspection['signature_base64']
            if ',' in sig_data:
                sig_data = sig_data.split(',')[-1]
            sig_bytes = base64.b64decode(sig_data)
            sig_img = RLImage(BytesIO(sig_bytes), width=2*inch, height=0.75*inch)
            elements.append(sig_img)
        except Exception as e:
            elements.append(Paragraph("Signature on file", normal_style))
        elements.append(Spacer(1, 12))
    
    # Declaration
    elements.append(Paragraph("Declaration", heading_style))
    if inspection['type'] == 'prestart':
        declaration_text = "I confirm this vehicle is safe to operate."
    else:
        declaration_text = "I confirm this report is accurate."
    elements.append(Paragraph(f"✓ {declaration_text}", normal_style))
    
    # Footer
    elements.append(Spacer(1, 30))
    footer_text = f"Generated by FleetShield365 | Report ID: {str(inspection.get('_id', 'N/A'))[:8]}"
    elements.append(Paragraph(footer_text, ParagraphStyle('Footer', fontSize=8, textColor=colors.gray)))
    
    doc.build(elements)
    pdf_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    return pdf_base64

# ============== SendGrid Email Service ==============

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

class EmailService:
    """SendGrid email service for sending notifications"""
    
    @staticmethod
    async def send_email(to_email: str, subject: str, body: str, company_id: str = None, is_html: bool = True):
        """
        Send email via SendGrid (if configured) or fallback to logging
        """
        sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
        sender_email = os.environ.get('SENDER_EMAIL', 'noreply@fleetguard.app')
        
        # Store in database for tracking
        email_log = {
            "to_email": to_email,
            "subject": subject,
            "body": body[:500],  # Truncate for storage
            "company_id": company_id,
            "sent_at": datetime.utcnow(),
            "status": "pending",
            "provider": "sendgrid" if sendgrid_api_key else "mock"
        }
        
        if sendgrid_api_key:
            try:
                message = Mail(
                    from_email=sender_email,
                    to_emails=to_email,
                    subject=subject,
                    html_content=body if is_html else None,
                    plain_text_content=body if not is_html else None
                )
                sg = SendGridAPIClient(sendgrid_api_key)
                response = sg.send(message)
                
                if response.status_code == 202:
                    email_log["status"] = "sent"
                    logger.info(f"[SENDGRID] Email sent to {to_email}: {subject}")
                else:
                    email_log["status"] = "failed"
                    email_log["error"] = f"Status code: {response.status_code}"
                    logger.error(f"[SENDGRID] Failed to send email: {response.status_code}")
            except Exception as e:
                email_log["status"] = "failed"
                email_log["error"] = str(e)
                logger.error(f"[SENDGRID] Error sending email: {e}")
        else:
            # Mock mode - just log it
            logger.info(f"[MOCK EMAIL] To: {to_email}")
            logger.info(f"[MOCK EMAIL] Subject: {subject}")
            logger.info(f"[MOCK EMAIL] Body: {body[:200]}...")
            email_log["status"] = "mocked"
        
        await db.email_logs.insert_one(email_log)
        return email_log["status"] in ["sent", "mocked"]
    
    @staticmethod
    async def send_alert_email(alert_type: str, message: str, admin_emails: list, company_id: str):
        """Send alert notification to admins with styled HTML"""
        subject_map = {
            "unsafe_vehicle": "🚨 URGENT: Vehicle Marked Unsafe",
            "repeated_issues": "⚠️ Alert: Repeated Vehicle Issues",
            "expiry_warning": "📅 Reminder: Upcoming Vehicle Expiry",
            "expiry_critical": "🚨 CRITICAL: Document Has Expired",
            "driver_expiry_warning": "📅 Reminder: Driver Document Expiring",
            "driver_expiry_critical": "🚨 CRITICAL: Driver Document Expired",
            "vehicle_offline": "🔴 Vehicle Status: Offline"
        }
        subject = subject_map.get(alert_type, "FleetShield365 Alert")
        
        # Determine alert color based on type
        alert_color = "#EF4444" if "critical" in alert_type or "unsafe" in alert_type else "#F59E0B"
        
        # Create HTML email body
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #F8FAFC; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <div style="background-color: {alert_color}; color: white; padding: 20px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">{subject}</h1>
                </div>
                <div style="padding: 30px;">
                    <p style="font-size: 16px; color: #334155; line-height: 1.6;">
                        {message}
                    </p>
                    <hr style="border: none; border-top: 1px solid #E2E8F0; margin: 20px 0;">
                    <p style="font-size: 14px; color: #64748B;">
                        This is an automated notification from FleetShield365.
                        Please log in to your dashboard to take action.
                    </p>
                </div>
                <div style="background-color: #F1F5F9; padding: 15px; text-align: center;">
                    <p style="margin: 0; font-size: 12px; color: #94A3B8;">
                        FleetShield365 - Vehicle Inspection Management
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        for email in admin_emails:
            await EmailService.send_email(email, subject, html_body, company_id, is_html=True)

email_service = EmailService()

# ============== Alert System ==============

async def check_and_create_expiry_alerts(vehicle: dict, company_id: str):
    """Check vehicle expiry dates and create alerts at 60, 30, 14, 7 day intervals"""
    now = datetime.utcnow()
    vehicle_name = f"{vehicle['name']} ({vehicle['registration_number']})"
    vehicle_id = str(vehicle['_id'])
    
    # Reminder intervals: 60, 30, 14, 7 days
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
                
                # Already expired
                if days_until_expiry < 0:
                    existing_alert = await db.alerts.find_one({
                        "vehicle_id": vehicle_id,
                        "type": "expiry_critical",
                        "message": {"$regex": f"{label}.*EXPIRED"}
                    })
                    
                    if not existing_alert:
                        message = f"🚨 {label} for {vehicle_name} has EXPIRED! (was due {expiry_date_str})"
                        await create_alert(company_id, "expiry_critical", message, vehicle_id)
                
                # Check each reminder interval
                else:
                    for reminder_day in REMINDER_DAYS:
                        if days_until_expiry <= reminder_day:
                            # Determine severity based on days remaining
                            if days_until_expiry <= 7:
                                alert_type = "expiry_critical"
                                urgency = "CRITICAL"
                                emoji = "🚨"
                            elif days_until_expiry <= 14:
                                alert_type = "expiry_warning"
                                urgency = "URGENT"
                                emoji = "⚠️"
                            elif days_until_expiry <= 30:
                                alert_type = "expiry_warning"
                                urgency = "ACTION NEEDED"
                                emoji = "📅"
                            else:  # 60 days
                                alert_type = "expiry_warning"
                                urgency = "HEADS UP"
                                emoji = "📋"
                            
                            # Check if alert already exists for this specific reminder
                            existing_alert = await db.alerts.find_one({
                                "vehicle_id": vehicle_id,
                                "type": alert_type,
                                "message": {"$regex": f"{label}.*{vehicle_name}.*{reminder_day}"}
                            })
                            
                            if not existing_alert:
                                message = f"{emoji} [{urgency}] {label} for {vehicle_name} expires in {days_until_expiry} days ({expiry_date_str})"
                                await create_alert(company_id, alert_type, message, vehicle_id)
                            
                            break  # Only create alert for the most urgent matching interval
                            
            except ValueError:
                pass  # Invalid date format, skip

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
    
    # Get admin emails
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

# ============== Auth Routes ==============

@api_router.post("/auth/register")
async def register(user: UserRegister, request: Request):
    # Check if email exists
    existing = await db.users.find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create company for super_admin
    company_id = user.company_id
    if user.role == UserRole.SUPER_ADMIN and not company_id:
        company = {
            "_id": ObjectId(),
            "name": f"{user.name}'s Company",
            "logo_base64": None,
            "subscription_plan": "basic",
            "active_vehicles_count": 0,
            "billing_history": [],
            "created_at": datetime.utcnow()
        }
        await db.companies.insert_one(company)
        company_id = str(company["_id"])
    
    # Create user
    user_doc = {
        "_id": ObjectId(),
        "email": user.email,
        "password_hash": get_password_hash(user.password),
        "name": user.name,
        "phone": user.phone,
        "role": user.role,
        "company_id": company_id,
        "assigned_vehicles": [],
        "created_at": datetime.utcnow(),
        "ip_address": request.client.host if request.client else "unknown"
    }
    await db.users.insert_one(user_doc)
    
    # Create token
    token = create_access_token({"sub": str(user_doc["_id"])})
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": serialize_doc(user_doc)
    }

@api_router.post("/auth/login")
async def login(credentials: UserLogin, request: Request):
    # Support login with email OR username
    login_identifier = credentials.email or credentials.username
    if not login_identifier:
        raise HTTPException(status_code=400, detail="Email or username is required")
    
    login_identifier = login_identifier.lower().strip()
    
    # Try to find user by email or username
    user = await db.users.find_one({
        "$or": [
            {"email": login_identifier},
            {"username": login_identifier}
        ]
    })
    
    if not user or not verify_password(credentials.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Update last login
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"last_login": datetime.utcnow(), "ip_address": request.client.host if request.client else "unknown"}}
    )
    
    # Token expiry based on "remember me" option
    expires_delta = timedelta(days=30) if credentials.remember_me else timedelta(days=1)
    token = create_access_token({"sub": str(user["_id"])}, expires_delta=expires_delta)
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": serialize_doc(user)
    }

@api_router.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    # Get company info if user has a company
    company = None
    if current_user.get("company_id"):
        company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    
    return {
        "user": serialize_doc(current_user),
        "company": serialize_doc(company) if company else None
    }

# ============== Password Reset ==============

class ForgotPasswordRequest(BaseModel):
    email: str
    origin_url: str = "https://fleet-shield-preview.preview.emergentagent.com"

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

@api_router.post("/auth/forgot-password")
async def forgot_password(request: ForgotPasswordRequest):
    """Send password reset email"""
    # Case-insensitive email lookup
    email_lower = request.email.lower()
    user = await db.users.find_one({"email": email_lower})
    
    # Always return success to prevent email enumeration
    if not user:
        return {"message": "If an account exists with this email, you will receive a password reset link."}
    
    # Generate reset token
    import secrets
    reset_token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(hours=1)
    
    # Store reset token
    await db.password_resets.update_one(
        {"user_id": str(user["_id"])},
        {"$set": {
            "user_id": str(user["_id"]),
            "token": reset_token,
            "expires_at": expires_at,
            "created_at": datetime.utcnow()
        }},
        upsert=True
    )
    
    # Send reset email
    reset_url = f"{request.origin_url}/reset-password?token={reset_token}"
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; background-color: #f8fafc;">
        <div style="max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px;">
            <h2 style="color: #0f172a; margin-bottom: 20px;">Reset Your Password</h2>
            <p style="color: #475569;">Hi {user.get('name', 'there')},</p>
            <p style="color: #475569;">We received a request to reset your FleetShield365 password. Click the button below to set a new password:</p>
            <div style="text-align: center; margin: 30px 0;">
                <a href="{reset_url}" style="background-color: #0d9488; color: white; padding: 12px 30px; text-decoration: none; border-radius: 8px; font-weight: bold;">Reset Password</a>
            </div>
            <p style="color: #94a3b8; font-size: 14px;">This link expires in 1 hour. If you didn't request this, you can safely ignore this email.</p>
            <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
            <p style="color: #94a3b8; font-size: 12px;">FleetShield365 - Equipment Inspection Management</p>
        </div>
    </body>
    </html>
    """
    
    await send_email_notification(
        request.email,
        "[FleetShield365] Reset Your Password",
        html_content
    )
    
    return {"message": "If an account exists with this email, you will receive a password reset link."}

@api_router.post("/auth/reset-password")
async def reset_password(request: ResetPasswordRequest):
    """Reset password using token"""
    reset_record = await db.password_resets.find_one({"token": request.token})
    
    if not reset_record:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    
    # Check expiration
    if datetime.utcnow() > reset_record["expires_at"]:
        await db.password_resets.delete_one({"token": request.token})
        raise HTTPException(status_code=400, detail="Reset token has expired")
    
    # Validate password
    if len(request.new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    # Update password
    await db.users.update_one(
        {"_id": ObjectId(reset_record["user_id"])},
        {"$set": {"password_hash": get_password_hash(request.new_password)}}
    )
    
    # Delete used token
    await db.password_resets.delete_one({"token": request.token})
    
    return {"message": "Password reset successfully. You can now log in with your new password."}

# ============== Email Test Route ==============

class TestEmailRequest(BaseModel):
    to_email: str

@api_router.post("/test-email")
async def test_email(request: TestEmailRequest, current_user: dict = Depends(get_current_user)):
    """Send a test email to verify SendGrid integration"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    html_content = """
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #0D9488;">FleetShield365 Email Test</h2>
        <p>This is a test email from FleetShield365.</p>
        <p>If you're receiving this, email notifications are working correctly!</p>
        <hr style="border: none; border-top: 1px solid #E5E7EB; margin: 20px 0;">
        <p style="color: #6B7280; font-size: 12px;">
            FleetShield365 - Vehicle Inspection Management
        </p>
    </body>
    </html>
    """
    
    success = await send_email_notification(
        request.to_email,
        "[FleetShield365] Test Email - Notifications Working!",
        html_content
    )
    
    if success:
        return {"status": "success", "message": f"Test email sent to {request.to_email}"}
    else:
        raise HTTPException(status_code=500, detail="Failed to send email. Check SendGrid configuration and sender verification.")

# ============== Company Routes ==============

@api_router.get("/company")
async def get_company(current_user: dict = Depends(get_current_user)):
    if not current_user.get("company_id"):
        raise HTTPException(status_code=404, detail="No company associated")
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    return serialize_doc(company)

@api_router.put("/company")
async def update_company(update: CompanyUpdate, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    if update_data:
        await db.companies.update_one(
            {"_id": ObjectId(current_user["company_id"])},
            {"$set": update_data}
        )
    
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    return serialize_doc(company)

@api_router.post("/company/logo")
async def upload_company_logo(logo: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    """Upload company logo for branding on PDF reports"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Validate file type
    if not logo.content_type.startswith('image/'):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    # Read file and convert to base64
    contents = await logo.read()
    if len(contents) > 5 * 1024 * 1024:  # 5MB limit
        raise HTTPException(status_code=400, detail="File too large (max 5MB)")
    
    # Store as base64 with data URI prefix
    logo_base64 = f"data:{logo.content_type};base64,{base64.b64encode(contents).decode('utf-8')}"
    
    # Update company with logo
    await db.companies.update_one(
        {"_id": ObjectId(current_user["company_id"])},
        {"$set": {"logo_base64": logo_base64, "logo_url": logo_base64}}
    )
    
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    return {"message": "Logo uploaded successfully", "logo_url": company.get("logo_url")}

# ============== User Management Routes ==============

class UserCreate(BaseModel):
    email: EmailStr
    full_name: str
    password: str
    role: str = "admin"

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None

@api_router.get("/users")
async def get_users(current_user: dict = Depends(get_current_user)):
    """Get all users in the company (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    users = await db.users.find({"company_id": current_user["company_id"]}).to_list(100)
    # Remove sensitive fields
    for user in users:
        user.pop("hashed_password", None)
    return serialize_doc(users)

@api_router.post("/users")
async def create_user(user_data: UserCreate, current_user: dict = Depends(get_current_user)):
    """Create a new admin user for the company"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Check if email already exists
    existing = await db.users.find_one({"email": user_data.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Hash password
    hashed_password = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    new_user = {
        "email": user_data.email,
        "full_name": user_data.full_name,
        "hashed_password": hashed_password,
        "role": user_data.role,
        "company_id": current_user["company_id"],
        "created_at": datetime.now(timezone.utc)
    }
    
    result = await db.users.insert_one(new_user)
    new_user["id"] = str(result.inserted_id)
    new_user.pop("hashed_password", None)
    new_user.pop("_id", None)
    return new_user

@api_router.put("/users/{user_id}")
async def update_user(user_id: str, user_data: UserUpdate, current_user: dict = Depends(get_current_user)):
    """Update a user (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Verify user belongs to same company
    user = await db.users.find_one({
        "_id": ObjectId(user_id),
        "company_id": current_user["company_id"]
    })
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    update_data = {k: v for k, v in user_data.dict().items() if v is not None}
    if update_data:
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})
    
    updated_user = await db.users.find_one({"_id": ObjectId(user_id)})
    updated_user.pop("hashed_password", None)
    return serialize_doc(updated_user)

@api_router.delete("/users/{user_id}")
async def delete_user(user_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a user (admin only, cannot delete self)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    if str(current_user["_id"]) == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    
    # Verify user belongs to same company
    user = await db.users.find_one({
        "_id": ObjectId(user_id),
        "company_id": current_user["company_id"]
    })
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    await db.users.delete_one({"_id": ObjectId(user_id)})
    return {"message": "User deleted"}

# ============== Vehicle Routes ==============

@api_router.post("/vehicles")
async def create_vehicle(vehicle: VehicleCreate, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    vehicle_doc = {
        "_id": ObjectId(),
        "company_id": current_user["company_id"],
        "name": vehicle.name,
        "registration_number": vehicle.registration_number,
        "trailer_attached": vehicle.trailer_attached,
        "status": vehicle.status,
        "rego_expiry": vehicle.rego_expiry,
        "insurance_expiry": vehicle.insurance_expiry,
        "safety_certificate_expiry": vehicle.safety_certificate_expiry,
        "coi_expiry": vehicle.coi_expiry,
        "service_due_km": vehicle.service_due_km,
        "current_odometer": vehicle.current_odometer or 0,
        "assigned_driver_ids": [],
        "created_at": datetime.utcnow()
    }
    await db.vehicles.insert_one(vehicle_doc)
    
    # Check for upcoming expiries and create alerts
    await check_and_create_expiry_alerts(vehicle_doc, current_user["company_id"])
    
    # Update company vehicle count
    await db.companies.update_one(
        {"_id": ObjectId(current_user["company_id"])},
        {"$inc": {"active_vehicles_count": 1}}
    )
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "vehicle", str(vehicle_doc["_id"]),
        request.client.host if request.client else "unknown"
    )
    
    return serialize_doc(vehicle_doc)

@api_router.get("/vehicles")
async def get_vehicles(current_user: dict = Depends(get_current_user)):
    query = {"company_id": current_user["company_id"]}
    
    # Drivers only see assigned vehicles
    if current_user["role"] == UserRole.DRIVER:
        query["assigned_driver_ids"] = str(current_user["_id"])
    
    vehicles = await db.vehicles.find(query).to_list(1000)
    return serialize_doc(vehicles)

@api_router.get("/vehicles/active-today")
async def get_active_vehicles_today(
    current_user: dict = Depends(get_current_user),
    tz_offset: int = 0
):
    """Lightweight endpoint to get just the IDs of vehicles that had inspections today"""
    company_id = current_user["company_id"]
    
    # Calculate "today" in client's timezone
    now_utc = datetime.utcnow()
    client_offset = timedelta(minutes=-tz_offset)
    client_now = now_utc + client_offset
    client_today_start = client_now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_utc = client_today_start - client_offset
    
    # Get active vehicle IDs
    active_ids = await db.inspections.distinct("vehicle_id", {
        "company_id": company_id,
        "timestamp": {"$gte": today_utc}
    })
    
    return {"active_vehicle_ids": active_ids, "count": len(active_ids)}

@api_router.get("/vehicles/{vehicle_id}")
async def get_vehicle(vehicle_id: str, current_user: dict = Depends(get_current_user)):
    vehicle = await db.vehicles.find_one({
        "_id": ObjectId(vehicle_id),
        "company_id": current_user["company_id"]
    })
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return serialize_doc(vehicle)

@api_router.put("/vehicles/{vehicle_id}")
async def update_vehicle(vehicle_id: str, update: VehicleUpdate, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    if update_data:
        await db.vehicles.update_one(
            {"_id": ObjectId(vehicle_id), "company_id": current_user["company_id"]},
            {"$set": update_data}
        )
    
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    
    await log_audit_trail(
        str(current_user["_id"]), "update", "vehicle", vehicle_id,
        request.client.host if request.client else "unknown", update_data
    )
    
    return serialize_doc(vehicle)

@api_router.delete("/vehicles/{vehicle_id}")
async def delete_vehicle(vehicle_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    result = await db.vehicles.delete_one({
        "_id": ObjectId(vehicle_id),
        "company_id": current_user["company_id"]
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    await db.companies.update_one(
        {"_id": ObjectId(current_user["company_id"])},
        {"$inc": {"active_vehicles_count": -1}}
    )
    
    await log_audit_trail(
        str(current_user["_id"]), "delete", "vehicle", vehicle_id,
        request.client.host if request.client else "unknown"
    )
    
    return {"message": "Vehicle deleted"}

@api_router.post("/vehicles/{vehicle_id}/assign")
async def assign_drivers(vehicle_id: str, assignment: DriverAssignment, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.vehicles.update_one(
        {"_id": ObjectId(vehicle_id), "company_id": current_user["company_id"]},
        {"$set": {"assigned_driver_ids": assignment.driver_ids}}
    )
    
    # Update drivers' assigned vehicles
    for driver_id in assignment.driver_ids:
        await db.users.update_one(
            {"_id": ObjectId(driver_id)},
            {"$addToSet": {"assigned_vehicles": vehicle_id}}
        )
    
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    return serialize_doc(vehicle)

# ============== Driver Routes ==============

@api_router.get("/drivers")
async def get_drivers(current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get regular drivers
    drivers = await db.users.find({
        "company_id": current_user["company_id"],
        "role": UserRole.DRIVER
    }).to_list(1000)
    
    # Also get admins who are enabled as operators
    admin_operators = await db.users.find({
        "company_id": current_user["company_id"],
        "role": {"$in": [UserRole.ADMIN, UserRole.SUPER_ADMIN]},
        "is_also_operator": True
    }).to_list(100)
    
    # Combine lists
    all_operators = drivers + admin_operators
    
    return serialize_doc(all_operators)

@api_router.post("/drivers")
async def create_driver(user: UserRegister, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Check if email is provided and already exists
    if user.email:
        existing = await db.users.find_one({"email": user.email.lower()})
        
        # Check if email belongs to an admin in the same company
        if existing:
            # If it's an admin from the same company, enable them as operator too
            if existing.get("role") in [UserRole.ADMIN, UserRole.SUPER_ADMIN] and existing.get("company_id") == current_user["company_id"]:
                # Add is_also_operator flag to the existing admin account
                await db.users.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "is_also_operator": True,
                        "operator_enabled_at": datetime.utcnow()
                    }}
                )
                # Return the updated user
                updated_user = await db.users.find_one({"_id": existing["_id"]})
                return serialize_doc(updated_user)
            else:
                raise HTTPException(status_code=400, detail="Email already registered")
    
    # Generate unique username
    username = user.username or await generate_unique_username(user.name, current_user["company_id"])
    
    # Check if username already exists in this company
    if await db.users.find_one({"username": username, "company_id": current_user["company_id"]}):
        username = await generate_unique_username(user.name, current_user["company_id"])
    
    driver_doc = {
        "_id": ObjectId(),
        "username": username,
        "password_hash": get_password_hash(user.password),
        "name": user.name,
        "phone": user.phone,
        "role": UserRole.DRIVER,
        "company_id": current_user["company_id"],
        "assigned_vehicles": [],
        "created_at": datetime.utcnow(),
        "ip_address": request.client.host if request.client else "unknown",
        # License and training details
        "license_number": user.license_number,
        "license_class": user.license_class,
        "license_expiry": user.license_expiry,
        "medical_certificate_expiry": user.medical_certificate_expiry,
        "first_aid_expiry": user.first_aid_expiry,
        "forklift_license_expiry": user.forklift_license_expiry,
        "dangerous_goods_expiry": user.dangerous_goods_expiry,
    }
    # Only add email if provided (sparse index doesn't like None values)
    if user.email:
        driver_doc["email"] = user.email.lower()
    
    await db.users.insert_one(driver_doc)
    
    return serialize_doc(driver_doc)

@api_router.delete("/drivers/{driver_id}")
async def delete_driver(driver_id: str, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    result = await db.users.delete_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"],
        "role": UserRole.DRIVER
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    return {"message": "Driver deleted"}

# ============== License Photo Routes (Owner Only) ==============

class LicensePhotoUpload(BaseModel):
    front_photo_base64: Optional[str] = None
    back_photo_base64: Optional[str] = None

class PasswordVerification(BaseModel):
    password: str

class DocumentDownloadRequest(BaseModel):
    operator_ids: List[str]
    document_types: List[str]  # driver_license, medical, first_aid, forklift, dangerous_goods
    password: str

@api_router.post("/drivers/download-documents")
async def download_operator_documents(request: DocumentDownloadRequest, current_user: dict = Depends(get_current_user)):
    """Download operator documents as ZIP - Owner (super_admin) only"""
    from fastapi.responses import StreamingResponse
    import base64
    import re
    
    # Only super_admin can download documents
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can download documents")
    
    # Verify password
    if not verify_password(request.password, current_user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid password")
    
    # Fetch selected operators
    operator_ids = [ObjectId(oid) for oid in request.operator_ids]
    operators = await db.users.find({
        "_id": {"$in": operator_ids},
        "company_id": current_user["company_id"]
    }).to_list(100)
    
    if not operators:
        raise HTTPException(status_code=404, detail="No operators found")
    
    # Create ZIP file in memory
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        manifest_lines = ["FleetShield365 Document Export", f"Generated: {datetime.now(timezone.utc).isoformat()}", ""]
        
        for operator in operators:
            op_name = operator.get("name", "Unknown").replace("/", "_").replace("\\", "_")
            folder_name = re.sub(r'[^\w\s-]', '', op_name).strip().replace(' ', '_')
            manifest_lines.append(f"\n{op_name}:")
            
            # Document type mappings
            doc_mappings = {
                "driver_license": [
                    ("license_photo_front", "driver_license_front.jpg"),
                    ("license_photo_back", "driver_license_back.jpg")
                ],
                "medical": [
                    ("medical_cert_front", "medical_certificate_front.jpg"),
                    ("medical_cert_back", "medical_certificate_back.jpg")
                ],
                "first_aid": [
                    ("first_aid_front", "first_aid_front.jpg"),
                    ("first_aid_back", "first_aid_back.jpg")
                ],
                "forklift": [
                    ("forklift_front", "forklift_license_front.jpg"),
                    ("forklift_back", "forklift_license_back.jpg")
                ],
                "dangerous_goods": [
                    ("dangerous_goods_front", "dangerous_goods_front.jpg"),
                    ("dangerous_goods_back", "dangerous_goods_back.jpg")
                ]
            }
            
            for doc_type in request.document_types:
                if doc_type not in doc_mappings:
                    continue
                    
                for field_name, file_name in doc_mappings[doc_type]:
                    photo_data = operator.get(field_name)
                    if photo_data:
                        # Handle base64 data
                        if photo_data.startswith("data:"):
                            # Extract base64 part after the comma
                            base64_data = photo_data.split(",", 1)[1] if "," in photo_data else photo_data
                        else:
                            base64_data = photo_data
                        
                        try:
                            image_bytes = base64.b64decode(base64_data)
                            zip_file.writestr(f"{folder_name}/{file_name}", image_bytes)
                            manifest_lines.append(f"  - {file_name}")
                        except Exception as e:
                            logger.error(f"Failed to decode image for {op_name}/{file_name}: {e}")
                            manifest_lines.append(f"  - {file_name} (ERROR: Could not decode)")
        
        # Add manifest
        zip_file.writestr("manifest.txt", "\n".join(manifest_lines))
    
    zip_buffer.seek(0)
    
    # Generate filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"FleetShield_Documents_{timestamp}.zip"
    
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
async def upload_license_photos(driver_id: str, photos: LicensePhotoUpload, current_user: dict = Depends(get_current_user)):
    """Upload license photos for a driver - Owner (super_admin) only"""
    # Only super_admin can upload license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can upload license photos")
    
    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    # Prepare update data
    update_data = {}
    if photos.front_photo_base64:
        update_data["license_photo_front"] = photos.front_photo_base64
    if photos.back_photo_base64:
        update_data["license_photo_back"] = photos.back_photo_base64
    
    if update_data:
        update_data["license_photos_updated_at"] = datetime.utcnow()
        update_data["license_photos_uploaded_by"] = str(current_user["_id"])
        await db.users.update_one(
            {"_id": ObjectId(driver_id)},
            {"$set": update_data}
        )
    
    return {"message": "License photos uploaded successfully", "updated_fields": list(update_data.keys())}

@api_router.post("/drivers/{driver_id}/license-photos/view")
async def view_license_photos(driver_id: str, verification: PasswordVerification, current_user: dict = Depends(get_current_user)):
    """View license photos with password re-authentication - Owner (super_admin) only"""
    # Only super_admin can view license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can view license photos")
    
    # Verify password
    if not verify_password(verification.password, current_user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid password")
    
    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    return {
        "driver_id": driver_id,
        "driver_name": driver.get("name"),
        "front_photo": driver.get("license_photo_front"),
        "back_photo": driver.get("license_photo_back"),
        "uploaded_at": driver.get("license_photos_updated_at")
    }

@api_router.delete("/drivers/{driver_id}/license-photos")
async def delete_license_photos(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Delete license photos for a driver - Owner (super_admin) only"""
    # Only super_admin can delete license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can delete license photos")
    
    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    await db.users.update_one(
        {"_id": ObjectId(driver_id)},
        {"$unset": {
            "license_photo_front": "",
            "license_photo_back": "",
            "license_photos_updated_at": "",
            "license_photos_uploaded_by": ""
        }}
    )
    
    return {"message": "License photos deleted successfully"}

@api_router.get("/drivers/{driver_id}/has-license-photos")
async def check_license_photos(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Check if driver has license photos - Owner (super_admin) only"""
    # Only super_admin can check license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can access license photo information")
    
    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    return {
        "has_front_photo": bool(driver.get("license_photo_front")),
        "has_back_photo": bool(driver.get("license_photo_back")),
        "uploaded_at": driver.get("license_photos_updated_at")
    }

# ============== Inspection Routes ==============

@api_router.post("/inspections/prestart")
async def create_prestart(inspection: PrestartCreate, request: Request, current_user: dict = Depends(get_current_user)):
    # Get vehicle
    vehicle = await db.vehicles.find_one({"_id": ObjectId(inspection.vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    # Check mandatory photos
    required_photos = {'front', 'rear', 'left', 'right', 'cabin', 'odometer'}
    provided_photos = {p.photo_type for p in inspection.photos}
    if not required_photos.issubset(provided_photos):
        missing = required_photos - provided_photos
        raise HTTPException(status_code=400, detail=f"Missing required photos: {missing}")
    
    # Check for issues requiring damage photos
    has_issues = any(item.status == ChecklistItemStatus.ISSUE for item in inspection.checklist_items)
    if has_issues and 'damage' not in provided_photos:
        raise HTTPException(status_code=400, detail="Damage photo required when issues are reported")
    
    inspection_id = ObjectId()
    
    # Store photos separately first (to avoid document size limit)
    photo_refs = []
    for photo in inspection.photos:
        photo_id = ObjectId()
        await db.inspection_photos.insert_one({
            "_id": photo_id,
            "inspection_id": str(inspection_id),
            "vehicle_id": inspection.vehicle_id,
            "photo_type": photo.photo_type,
            "base64_data": photo.base64_data,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
            "ai_damage_status": photo.ai_damage_status,
            "inspection_type": InspectionType.PRESTART,
            "created_at": datetime.utcnow()
        })
        # Store only reference in main document
        photo_refs.append({
            "photo_id": str(photo_id),
            "photo_type": photo.photo_type,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
        })
    
    inspection_doc = {
        "_id": inspection_id,
        "vehicle_id": inspection.vehicle_id,
        "driver_id": str(current_user["_id"]),
        "company_id": current_user["company_id"],
        "type": InspectionType.PRESTART,
        "odometer": inspection.odometer,
        "checklist_items": [item.dict() for item in inspection.checklist_items],
        "photo_refs": photo_refs,  # Only store references, not full base64
        "signature_base64": inspection.signature_base64,
        "declaration_confirmed": inspection.declaration_confirmed,
        "gps_latitude": inspection.gps_latitude,
        "gps_longitude": inspection.gps_longitude,
        "timestamp": datetime.utcnow(),
        "ip_address": request.client.host if request.client else "unknown",
        "pdf_base64": None,
        "is_safe": not has_issues
    }
    
    await db.inspections.insert_one(inspection_doc)
    
    # Update vehicle odometer
    await db.vehicles.update_one(
        {"_id": ObjectId(inspection.vehicle_id)},
        {"$set": {"current_odometer": inspection.odometer}}
    )
    
    # Generate PDF
    driver = await db.users.find_one({"_id": current_user["_id"]})
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    pdf_base64 = await generate_inspection_pdf(inspection_doc, vehicle, driver, company)
    
    await db.inspections.update_one(
        {"_id": inspection_doc["_id"]},
        {"$set": {"pdf_base64": pdf_base64}}
    )
    inspection_doc["pdf_base64"] = pdf_base64
    
    # Create alert if vehicle marked unsafe
    if has_issues:
        issue_items = [item.name for item in inspection.checklist_items if item.status == ChecklistItemStatus.ISSUE]
        await create_alert(
            current_user["company_id"],
            "unsafe_vehicle",
            f"Vehicle {vehicle['name']} ({vehicle['registration_number']}) has issues: {', '.join(issue_items)}",
            inspection.vehicle_id,
            str(current_user["_id"])
        )
        
        # Send notifications to admins
        await notify_admins(
            current_user["company_id"],
            "issue",
            f"Issue Reported: {vehicle['name']}",
            f"Driver reported: {', '.join(issue_items)}",
            {"inspection_id": str(inspection_id), "vehicle_id": inspection.vehicle_id},
            send_issue_alert_email,
            (vehicle['name'], current_user.get('full_name', 'Driver'), ', '.join(issue_items), "Pre-start")
        )
    
    # Check for repeated issues (3+ in 7 days)
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    recent_issues = await db.inspections.count_documents({
        "vehicle_id": inspection.vehicle_id,
        "is_safe": False,
        "timestamp": {"$gte": seven_days_ago}
    })
    
    if recent_issues >= 3:
        await create_alert(
            current_user["company_id"],
            "repeated_issues",
            f"Vehicle {vehicle['name']} has had {recent_issues} issues in the last 7 days",
            inspection.vehicle_id
        )
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "inspection", str(inspection_doc["_id"]),
        request.client.host if request.client else "unknown"
    )
    
    return serialize_doc(inspection_doc)

@api_router.post("/inspections/end-shift")
async def create_end_shift(inspection: EndShiftCreate, request: Request, current_user: dict = Depends(get_current_user)):
    vehicle = await db.vehicles.find_one({"_id": ObjectId(inspection.vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    # Validate damage/incident photos
    if inspection.new_damage and not any(p.photo_type == 'damage' for p in (inspection.photos or [])):
        raise HTTPException(status_code=400, detail="Damage photo required when new damage reported")
    
    inspection_id = ObjectId()
    
    # Store photos separately first (to avoid document size limit)
    photo_refs = []
    for photo in (inspection.photos or []):
        photo_id = ObjectId()
        await db.inspection_photos.insert_one({
            "_id": photo_id,
            "inspection_id": str(inspection_id),
            "vehicle_id": inspection.vehicle_id,
            "photo_type": photo.photo_type,
            "base64_data": photo.base64_data,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
            "ai_damage_status": photo.ai_damage_status,
            "inspection_type": InspectionType.END_SHIFT,
            "created_at": datetime.utcnow()
        })
        photo_refs.append({
            "photo_id": str(photo_id),
            "photo_type": photo.photo_type,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
        })
    
    inspection_doc = {
        "_id": inspection_id,
        "vehicle_id": inspection.vehicle_id,
        "driver_id": str(current_user["_id"]),
        "company_id": current_user["company_id"],
        "type": InspectionType.END_SHIFT,
        "odometer": inspection.odometer,
        "fuel_level": inspection.fuel_level,
        "new_damage": inspection.new_damage,
        "incident_today": inspection.incident_today,
        "cleanliness": inspection.cleanliness,
        "damage_comment": inspection.damage_comment,
        "incident_comment": inspection.incident_comment,
        "photo_refs": photo_refs,  # Only store references, not full base64
        "signature_base64": inspection.signature_base64,
        "declaration_confirmed": inspection.declaration_confirmed,
        "gps_latitude": inspection.gps_latitude,
        "gps_longitude": inspection.gps_longitude,
        "timestamp": datetime.utcnow(),
        "ip_address": request.client.host if request.client else "unknown",
        "pdf_base64": None
    }
    
    await db.inspections.insert_one(inspection_doc)
    
    # Update vehicle odometer
    await db.vehicles.update_one(
        {"_id": ObjectId(inspection.vehicle_id)},
        {"$set": {"current_odometer": inspection.odometer}}
    )
    
    # Generate PDF
    driver = await db.users.find_one({"_id": current_user["_id"]})
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    pdf_base64 = await generate_inspection_pdf(inspection_doc, vehicle, driver, company)
    
    await db.inspections.update_one(
        {"_id": inspection_doc["_id"]},
        {"$set": {"pdf_base64": pdf_base64}}
    )
    inspection_doc["pdf_base64"] = pdf_base64
    
    # Create alert if damage or incident
    if inspection.new_damage:
        await create_alert(
            current_user["company_id"],
            "unsafe_vehicle",
            f"New damage reported on {vehicle['name']}: {inspection.damage_comment or 'No details'}",
            inspection.vehicle_id,
            str(current_user["_id"])
        )
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "inspection", str(inspection_doc["_id"]),
        request.client.host if request.client else "unknown"
    )
    
    return serialize_doc(inspection_doc)

@api_router.get("/inspections")
async def get_inspections(
    vehicle_id: Optional[str] = None,
    driver_id: Optional[str] = None,
    inspection_type: Optional[str] = None,
    has_issues: Optional[bool] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_photos: Optional[bool] = False,
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    query = {"company_id": current_user["company_id"]}
    
    # Drivers only see their own inspections
    if current_user["role"] == UserRole.DRIVER:
        query["driver_id"] = str(current_user["_id"])
    elif driver_id:
        query["driver_id"] = driver_id
    
    if vehicle_id:
        query["vehicle_id"] = vehicle_id
    if inspection_type:
        query["type"] = inspection_type
    if has_issues is not None:
        query["is_safe"] = not has_issues
    if start_date:
        query["timestamp"] = {"$gte": datetime.fromisoformat(start_date)}
    if end_date:
        if "timestamp" in query:
            query["timestamp"]["$lte"] = datetime.fromisoformat(end_date)
        else:
            query["timestamp"] = {"$lte": datetime.fromisoformat(end_date)}
    
    # Cap at 500 for performance
    actual_limit = min(limit, 500)
    
    # Exclude large base64 data from list query for performance
    projection = {"signature_base64": 0, "photos": 0, "pdf_base64": 0, "photo_refs": 0}
    inspections = await db.inspections.find(query, projection).sort("timestamp", -1).to_list(actual_limit)
    
    # Optionally include photos (only when viewing single inspection detail)
    if include_photos:
        for inspection in inspections:
            photos = await fetch_inspection_photos(str(inspection["_id"]))
            inspection["photos"] = photos
    
    return serialize_doc(inspections)

async def fetch_inspection_photos(inspection_id: str) -> List[dict]:
    """Fetch photos for an inspection from the separate collection"""
    # First try to find photos by report_id
    photos = await db.inspection_photos.find({"report_id": inspection_id}).to_list(20)
    
    if not photos:
        # If no photos found by report_id, check if inspection has photo_refs
        inspection = await db.inspections.find_one({"_id": ObjectId(inspection_id)})
        if inspection and inspection.get("photo_refs"):
            photo_ids = [ref.get("photo_id") for ref in inspection["photo_refs"] if ref.get("photo_id")]
            if photo_ids:
                # Fetch photos by their IDs
                photos = await db.inspection_photos.find({
                    "_id": {"$in": [ObjectId(pid) for pid in photo_ids]}
                }).to_list(20)
    
    return [{"photo_type": p.get("photo_type"), "base64_data": p.get("base64_data")} for p in photos]

@api_router.get("/inspections/{inspection_id}")
async def get_inspection(inspection_id: str, current_user: dict = Depends(get_current_user)):
    inspection = await db.inspections.find_one({
        "_id": ObjectId(inspection_id),
        "company_id": current_user["company_id"]
    })
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    
    # Fetch photos from separate collection
    photos = await fetch_inspection_photos(inspection_id)
    inspection["photos"] = photos
    
    return serialize_doc(inspection)

@api_router.get("/inspections/{inspection_id}/pdf")
async def get_inspection_pdf(inspection_id: str, regenerate: bool = False, current_user: dict = Depends(get_current_user)):
    inspection = await db.inspections.find_one({
        "_id": ObjectId(inspection_id),
        "company_id": current_user["company_id"]
    })
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    
    # If regenerate requested or no PDF exists, regenerate with photos
    if regenerate or not inspection.get("pdf_base64"):
        # Fetch photos
        photos = await fetch_inspection_photos(inspection_id)
        inspection["photos"] = photos
        
        # Fetch related data
        vehicle = await db.vehicles.find_one({"_id": ObjectId(inspection["vehicle_id"])})
        driver = await db.users.find_one({"_id": ObjectId(inspection["driver_id"])})
        company = await db.companies.find_one({"_id": ObjectId(inspection["company_id"])})
        
        # Generate PDF with photos
        pdf_base64 = await generate_inspection_pdf(inspection, vehicle, driver, company)
        
        # Update stored PDF
        await db.inspections.update_one(
            {"_id": ObjectId(inspection_id)},
            {"$set": {"pdf_base64": pdf_base64}}
        )
        
        return {"pdf_base64": pdf_base64}
    
    return {"pdf_base64": inspection["pdf_base64"]}

# ============== Fuel Submission Routes ==============

@api_router.post("/fuel")
async def create_fuel_submission(fuel: FuelSubmission, request: Request, current_user: dict = Depends(get_current_user)):
    """Driver submits fuel receipt"""
    vehicle = await db.vehicles.find_one({"_id": ObjectId(fuel.vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    fuel_doc = {
        "_id": ObjectId(),
        "company_id": current_user["company_id"],
        "vehicle_id": fuel.vehicle_id,
        "driver_id": str(current_user["_id"]),
        "amount": fuel.amount,
        "liters": fuel.liters,
        "price_per_liter": round(fuel.amount / fuel.liters, 2) if fuel.liters > 0 else 0,
        "receipt_photo_base64": fuel.receipt_photo_base64,
        "odometer": fuel.odometer,
        "fuel_station": fuel.fuel_station,
        "notes": fuel.notes,
        "timestamp": datetime.utcnow(),
        "ip_address": request.client.host if request.client else "unknown"
    }
    
    await db.fuel_submissions.insert_one(fuel_doc)
    
    return {"id": str(fuel_doc["_id"]), "message": "Fuel submission recorded successfully"}

@api_router.get("/fuel")
async def get_fuel_submissions(vehicle_id: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    """Get fuel submissions for company"""
    query = {"company_id": current_user["company_id"]}
    if vehicle_id:
        query["vehicle_id"] = vehicle_id
    
    # Exclude large base64 images from list query for performance
    projection = {"receipt_photo_base64": 0}
    submissions = await db.fuel_submissions.find(query, projection).sort("timestamp", -1).to_list(100)
    
    # Get vehicle names
    vehicle_ids = list(set(s["vehicle_id"] for s in submissions))
    vehicles = await db.vehicles.find({"_id": {"$in": [ObjectId(vid) for vid in vehicle_ids]}}).to_list(100)
    vehicle_map = {str(v["_id"]): v["name"] for v in vehicles}
    
    # Get driver names
    driver_ids = list(set(s["driver_id"] for s in submissions))
    drivers = await db.users.find({"_id": {"$in": [ObjectId(did) for did in driver_ids]}}).to_list(100)
    driver_map = {str(d["_id"]): d["name"] for d in drivers}
    
    for s in submissions:
        s["id"] = str(s.pop("_id"))
        s["vehicle_name"] = vehicle_map.get(s["vehicle_id"], "Unknown")
        s["driver_name"] = driver_map.get(s["driver_id"], "Unknown")
        s["has_receipt"] = True  # Indicate receipt exists but not included
    
    return submissions

# ============== Driver Update Routes ==============

@api_router.put("/drivers/{driver_id}")
async def update_driver(driver_id: str, update: DriverUpdate, request: Request, current_user: dict = Depends(get_current_user)):
    """Update driver details including license and training"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    driver = await db.users.find_one({"_id": ObjectId(driver_id), "company_id": current_user["company_id"]})
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    if update_data:
        await db.users.update_one({"_id": ObjectId(driver_id)}, {"$set": update_data})
        
        # Check for expiring documents and create alerts
        await check_driver_expiry_alerts(driver_id, current_user["company_id"])
    
    return {"message": "Driver updated successfully"}

@api_router.post("/drivers/{driver_id}/send-credentials")
async def send_driver_credentials(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Send login credentials to driver via email"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    driver = await db.users.find_one({"_id": ObjectId(driver_id), "company_id": current_user["company_id"]})
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    company_name = company.get("name", "FleetShield365") if company else "FleetShield365"
    
    driver_email = driver.get("email")
    driver_name = driver.get("name", "Operator")
    
    if not driver_email:
        raise HTTPException(status_code=400, detail="Driver has no email address")
    
    # Create welcome email with login instructions
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; background-color: #f8fafc;">
        <div style="max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px;">
            <h2 style="color: #0f172a; margin-bottom: 20px;">Welcome to FleetShield365!</h2>
            <p style="color: #475569;">Hi {driver_name},</p>
            <p style="color: #475569;">You've been added as an operator for <strong>{company_name}</strong>. You can now access the FleetShield365 mobile app to complete equipment inspections.</p>
            
            <div style="background-color: #f1f5f9; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="color: #0f172a; margin-top: 0;">Your Login Details:</h3>
                <p style="color: #475569; margin: 5px 0;"><strong>Email:</strong> {driver_email}</p>
                <p style="color: #475569; margin: 5px 0;"><strong>Password:</strong> (set by your admin)</p>
            </div>
            
            <p style="color: #475569;">If you don't know your password, please contact your administrator.</p>
            
            <div style="text-align: center; margin: 30px 0;">
                <a href="https://fleetshield365.com" style="background-color: #0d9488; color: white; padding: 12px 30px; text-decoration: none; border-radius: 8px; font-weight: bold;">Open FleetShield365</a>
            </div>
            
            <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
            <p style="color: #94a3b8; font-size: 12px;">FleetShield365 - Equipment Inspection Management</p>
        </div>
    </body>
    </html>
    """
    
    success = await send_email_notification(
        driver_email,
        f"[FleetShield365] Your Login Credentials for {company_name}",
        html_content
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to send email. Please check email configuration.")
    
    return {"message": f"Credentials sent to {driver_email}"}

async def check_driver_expiry_alerts(driver_id: str, company_id: str):
    """Check driver document expiry dates and create alerts at 60, 30, 14, 7 day intervals"""
    driver = await db.users.find_one({"_id": ObjectId(driver_id)})
    if not driver:
        return
    
    driver_name = driver.get("name", "Unknown Driver")
    now = datetime.utcnow()
    
    # Reminder intervals: 60, 30, 14, 7 days
    REMINDER_DAYS = [60, 30, 14, 7]
    
    expiry_fields = [
        ('license_expiry', 'Driver License'),
        ('medical_certificate_expiry', 'Medical Certificate'),
        ('first_aid_expiry', 'First Aid Certificate'),
        ('forklift_license_expiry', 'Forklift License'),
        ('dangerous_goods_expiry', 'Dangerous Goods Training'),
    ]
    
    for field, label in expiry_fields:
        expiry_str = driver.get(field)
        if expiry_str and expiry_str.upper() != "NA":
            try:
                expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                days_until = (expiry_date - now).days
                
                # Already expired
                if days_until < 0:
                    existing = await db.alerts.find_one({
                        "driver_id": driver_id,
                        "type": "driver_expiry_critical",
                        "message": {"$regex": f"{label}.*EXPIRED"}
                    })
                    if not existing:
                        message = f"🚨 {label} for {driver_name} has EXPIRED! (was due {expiry_str})"
                        await create_alert(company_id, "driver_expiry_critical", message, driver_id=driver_id)
                        await send_driver_expiry_email(company_id, driver_name, label, days_until, expiry_str, expired=True)
                
                # Check each reminder interval
                else:
                    for reminder_day in REMINDER_DAYS:
                        if days_until <= reminder_day:
                            # Determine severity based on days remaining
                            if days_until <= 7:
                                alert_type = "driver_expiry_critical"
                                urgency = "CRITICAL"
                                emoji = "🚨"
                            elif days_until <= 14:
                                alert_type = "driver_expiry_warning"
                                urgency = "URGENT"
                                emoji = "⚠️"
                            elif days_until <= 30:
                                alert_type = "driver_expiry_warning"
                                urgency = "ACTION NEEDED"
                                emoji = "📅"
                            else:  # 60 days
                                alert_type = "driver_expiry_warning"
                                urgency = "HEADS UP"
                                emoji = "📋"
                            
                            # Check if alert already exists for this specific reminder
                            existing = await db.alerts.find_one({
                                "driver_id": driver_id,
                                "type": alert_type,
                                "message": {"$regex": f"{label}.*{driver_name}.*{reminder_day}"}
                            })
                            
                            if not existing:
                                message = f"{emoji} [{urgency}] {label} for {driver_name} expires in {days_until} days ({expiry_str})"
                                await create_alert(company_id, alert_type, message, driver_id=driver_id)
                                await send_driver_expiry_email(company_id, driver_name, label, days_until, expiry_str)
                            
                            break  # Only create alert for the most urgent matching interval
                            
            except ValueError:
                pass

async def send_driver_expiry_email(company_id: str, driver_name: str, document_type: str, days_until: int, expiry_date: str, expired: bool = False):
    """Send email notification about driver document expiry"""
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": [UserRole.SUPER_ADMIN, UserRole.ADMIN]}
    }).to_list(100)
    
    for admin in admins:
        if expired:
            subject = f"🚨 URGENT: {driver_name}'s {document_type} has EXPIRED"
            body = f"URGENT: {driver_name}'s {document_type} expired on {expiry_date}.\n\nPlease ensure this is updated immediately to maintain compliance."
        else:
            subject = f"⚠️ Reminder: {driver_name}'s {document_type} expires in {days_until} days"
            body = f"{driver_name}'s {document_type} will expire on {expiry_date} ({days_until} days remaining).\n\nPlease arrange renewal before expiry."
        
        await send_email_notification(admin.get("email"), subject, body)

# ============== Maintenance Routes ==============

@api_router.post("/maintenance")
async def create_maintenance(log: MaintenanceLogCreate, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    maintenance_doc = {
        "_id": ObjectId(),
        "company_id": current_user["company_id"],
        "vehicle_id": log.vehicle_id,
        "service_date": log.service_date,
        "service_type": log.service_type,
        "cost": log.cost,
        "workshop_name": log.workshop_name,
        "invoice_base64": log.invoice_base64,
        "notes": log.notes,
        "created_by": str(current_user["_id"]),
        "created_at": datetime.utcnow()
    }
    await db.maintenance_logs.insert_one(maintenance_doc)
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "maintenance", str(maintenance_doc["_id"]),
        request.client.host if request.client else "unknown"
    )
    
    return serialize_doc(maintenance_doc)

@api_router.get("/maintenance")
async def get_maintenance_logs(vehicle_id: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    query = {"company_id": current_user["company_id"]}
    if vehicle_id:
        query["vehicle_id"] = vehicle_id
    
    logs = await db.maintenance_logs.find(query).sort("service_date", -1).to_list(1000)
    return serialize_doc(logs)

@api_router.get("/maintenance/stats/{vehicle_id}")
async def get_maintenance_stats(vehicle_id: str, current_user: dict = Depends(get_current_user)):
    logs = await db.maintenance_logs.find({
        "company_id": current_user["company_id"],
        "vehicle_id": vehicle_id
    }).to_list(1000)
    
    total_cost = sum(log.get("cost", 0) for log in logs)
    service_count = len(logs)
    
    return {
        "vehicle_id": vehicle_id,
        "total_cost": total_cost,
        "service_count": service_count,
        "logs": serialize_doc(logs)
    }

# ============== Alert Routes ==============

@api_router.get("/alerts")
async def get_alerts(unread_only: bool = False, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    query = {"company_id": current_user["company_id"]}
    if unread_only:
        query["is_read"] = False
    
    alerts = await db.alerts.find(query).sort("created_at", -1).to_list(1000)
    return serialize_doc(alerts)

@api_router.put("/alerts/{alert_id}/read")
async def mark_alert_read(alert_id: str, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.alerts.update_one(
        {"_id": ObjectId(alert_id), "company_id": current_user["company_id"]},
        {"$set": {"is_read": True}}
    )
    return {"message": "Alert marked as read"}

# ============== Incident Reports ==============

async def send_incident_alert_email(admin_email: str, company_name: str, incident: dict, vehicle_name: str, driver_name: str):
    """Send incident alert email to admin"""
    severity_colors = {
        "minor": "#F59E0B",
        "moderate": "#F97316", 
        "severe": "#DC2626"
    }
    severity_color = severity_colors.get(incident.get("severity", "moderate"), "#F97316")
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: {severity_color};">🚨 INCIDENT REPORT - {incident.get('severity', 'MODERATE').upper()}</h2>
        <p>Hi {company_name} Admin,</p>
        <p><strong>An incident has been reported and requires your immediate attention.</strong></p>
        
        <div style="background-color: #FEF2F2; border: 1px solid #FECACA; padding: 16px; border-radius: 8px; margin: 20px 0;">
            <p><strong>Vehicle:</strong> {vehicle_name}</p>
            <p><strong>Driver:</strong> {driver_name}</p>
            <p><strong>Date/Time:</strong> {incident.get('created_at', 'N/A')}</p>
            <p><strong>Location:</strong> {incident.get('location_address', 'GPS coordinates available')}</p>
            <p><strong>Injuries:</strong> {'Yes - ' + incident.get('injury_description', '') if incident.get('injuries_occurred') else 'No injuries reported'}</p>
        </div>
        
        <h3>Description:</h3>
        <p style="background-color: #F8FAFC; padding: 12px; border-radius: 4px;">{incident.get('description', 'No description provided')}</p>
        
        <h3>Other Party Details:</h3>
        <p><strong>Name:</strong> {incident.get('other_party', {}).get('name', 'N/A')}</p>
        <p><strong>Phone:</strong> {incident.get('other_party', {}).get('phone', 'N/A')}</p>
        <p><strong>Vehicle Rego:</strong> {incident.get('other_party', {}).get('vehicle_rego', 'N/A')}</p>
        
        <p style="margin-top: 20px;"><strong>Please log in to FleetShield365 to view full details, photos, and take action.</strong></p>
        <p style="color: #64748B; font-size: 12px;">This is an automated alert from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[URGENT] Incident Report: {vehicle_name} - {incident.get('severity', 'moderate').upper()}", html_content)

@api_router.post("/incidents")
async def create_incident(
    incident: IncidentCreate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Create a new incident report"""
    company_id = current_user["company_id"]
    
    # Get vehicle info
    vehicle = await db.vehicles.find_one({"_id": ObjectId(incident.vehicle_id), "company_id": company_id})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    incident_doc = {
        "company_id": company_id,
        "vehicle_id": incident.vehicle_id,
        "driver_id": str(current_user["_id"]),
        "description": incident.description,
        "severity": incident.severity,
        "location_address": incident.location_address,
        "gps_latitude": incident.gps_latitude,
        "gps_longitude": incident.gps_longitude,
        "other_party": incident.other_party.dict(),
        "witnesses": [w.dict() for w in incident.witnesses] if incident.witnesses else [],
        "police_report_number": incident.police_report_number,
        "injuries_occurred": incident.injuries_occurred,
        "injury_description": incident.injury_description,
        "damage_photos": incident.damage_photos,
        "other_vehicle_photos": incident.other_vehicle_photos,
        "scene_photos": incident.scene_photos,
        "status": "reported",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    
    result = await db.incidents.insert_one(incident_doc)
    incident_doc["id"] = str(result.inserted_id)
    
    # Create alert for admin
    alert_doc = {
        "company_id": company_id,
        "type": "incident_report",
        "severity": "critical" if incident.severity == "severe" else "warning",
        "message": f"Incident reported: {vehicle.get('name', 'Unknown')} - {incident.severity.upper()} - {incident.description[:100]}",
        "vehicle_id": incident.vehicle_id,
        "driver_id": str(current_user["_id"]),
        "incident_id": str(result.inserted_id),
        "is_read": False,
        "created_at": datetime.now(timezone.utc),
    }
    await db.alerts.insert_one(alert_doc)
    
    # Send email notification to admins
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    admin_users = await db.users.find({
        "company_id": company_id,
        "role": {"$in": [UserRole.ADMIN, UserRole.SUPER_ADMIN]}
    }).to_list(100)
    
    vehicle_name = f"{vehicle.get('name', 'Unknown')} ({vehicle.get('registration_number', 'N/A')})"
    driver_name = current_user.get("name", current_user.get("email", "Unknown"))
    
    for admin in admin_users:
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])})
        if prefs and prefs.get("email_issue_alerts", True):
            background_tasks.add_task(
                send_incident_alert_email,
                admin["email"],
                company.get("name", "Your Company") if company else "Your Company",
                incident_doc,
                vehicle_name,
                driver_name
            )
    
    # Send push notification to admins
    push_tokens = []
    for admin in admin_users:
        tokens = await db.push_tokens.find({"user_id": str(admin["_id"])}).to_list(10)
        push_tokens.extend([t["token"] for t in tokens])
    
    if push_tokens:
        background_tasks.add_task(
            send_push_notification,
            push_tokens,
            f"🚨 Incident Report - {incident.severity.upper()}",
            f"{vehicle_name}: {incident.description[:100]}",
            {"type": "incident", "incident_id": str(result.inserted_id)}
        )
    
    return serialize_doc(incident_doc)

@api_router.get("/incidents")
async def get_incidents(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = None,
    severity: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    limit: int = 50,
    skip: int = 0
):
    """Get all incidents for the company"""
    company_id = current_user["company_id"]
    
    query = {"company_id": company_id}
    if status:
        query["status"] = status
    if severity:
        query["severity"] = severity
    if vehicle_id:
        query["vehicle_id"] = vehicle_id
    
    incidents = await db.incidents.find(query).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    # Enrich with vehicle and driver info
    for incident in incidents:
        vehicle = await db.vehicles.find_one({"_id": ObjectId(incident["vehicle_id"])})
        driver = await db.users.find_one({"_id": ObjectId(incident["driver_id"])})
        incident["vehicle_name"] = vehicle.get("name", "Unknown") if vehicle else "Unknown"
        incident["vehicle_rego"] = vehicle.get("registration_number", "N/A") if vehicle else "N/A"
        incident["driver_name"] = driver.get("name", driver.get("email", "Unknown")) if driver else "Unknown"
    
    return serialize_doc(incidents)

@api_router.get("/incidents/{incident_id}")
async def get_incident(incident_id: str, current_user: dict = Depends(get_current_user)):
    """Get a specific incident by ID"""
    company_id = current_user["company_id"]
    
    incident = await db.incidents.find_one({
        "_id": ObjectId(incident_id),
        "company_id": company_id
    })
    
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    
    # Enrich with vehicle and driver info
    vehicle = await db.vehicles.find_one({"_id": ObjectId(incident["vehicle_id"])})
    driver = await db.users.find_one({"_id": ObjectId(incident["driver_id"])})
    incident["vehicle_name"] = vehicle.get("name", "Unknown") if vehicle else "Unknown"
    incident["vehicle_rego"] = vehicle.get("registration_number", "N/A") if vehicle else "N/A"
    incident["driver_name"] = driver.get("name", driver.get("email", "Unknown")) if driver else "Unknown"
    
    return serialize_doc(incident)

@api_router.put("/incidents/{incident_id}")
async def update_incident(
    incident_id: str,
    update: IncidentUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update an incident (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    company_id = current_user["company_id"]
    
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    update_data["updated_at"] = datetime.now(timezone.utc)
    
    result = await db.incidents.update_one(
        {"_id": ObjectId(incident_id), "company_id": company_id},
        {"$set": update_data}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Incident not found")
    
    return {"message": "Incident updated"}

@api_router.get("/incidents/stats/summary")
async def get_incident_stats(current_user: dict = Depends(get_current_user)):
    """Get incident statistics for dashboard"""
    company_id = current_user["company_id"]
    
    # Total incidents
    total = await db.incidents.count_documents({"company_id": company_id})
    
    # This month
    month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    this_month = await db.incidents.count_documents({
        "company_id": company_id,
        "created_at": {"$gte": month_start}
    })
    
    # By severity
    by_severity = {}
    for sev in ["minor", "moderate", "severe"]:
        by_severity[sev] = await db.incidents.count_documents({
            "company_id": company_id,
            "severity": sev
        })
    
    # By status
    by_status = {}
    for status in ["reported", "under_review", "resolved", "closed"]:
        by_status[status] = await db.incidents.count_documents({
            "company_id": company_id,
            "status": status
        })
    
    # Open incidents (not resolved/closed)
    open_incidents = await db.incidents.count_documents({
        "company_id": company_id,
        "status": {"$in": ["reported", "under_review"]}
    })
    
    return {
        "total": total,
        "this_month": this_month,
        "open_incidents": open_incidents,
        "by_severity": by_severity,
        "by_status": by_status
    }

# ============== Dashboard Stats ==============

@api_router.get("/dashboard/stats")
async def get_dashboard_stats(
    current_user: dict = Depends(get_current_user),
    tz_offset: int = 0  # Timezone offset in minutes from UTC (e.g., -600 for AEST)
):
    company_id = current_user["company_id"]
    
    # Check cache first for faster response
    cached = get_cached_stats(company_id)
    if cached:
        return cached
    
    # Calculate "today" in client's timezone
    now_utc = datetime.utcnow()
    client_offset = timedelta(minutes=-tz_offset)
    client_now = now_utc + client_offset
    client_today_start = client_now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_utc = client_today_start - client_offset
    
    # Pre-calculate date strings
    thirty_days = (datetime.utcnow() + timedelta(days=30)).isoformat()[:10]
    sixty_days = (datetime.utcnow() + timedelta(days=60)).isoformat()[:10]
    today_str = datetime.utcnow().isoformat()[:10]
    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Run all queries in parallel for better performance
    results = await asyncio.gather(
        # Basic counts
        db.vehicles.count_documents({"company_id": company_id}),
        db.inspections.count_documents({"company_id": company_id, "timestamp": {"$gte": today_utc}}),
        db.inspections.distinct("vehicle_id", {"company_id": company_id, "timestamp": {"$gte": today_utc}}),
        db.inspections.count_documents({"company_id": company_id, "timestamp": {"$gte": today_utc}, "is_safe": False}),
        db.vehicles.count_documents({"company_id": company_id, "status": {"$in": [VehicleStatus.REGO_EXPIRED, VehicleStatus.SAFETY_INSPECTION_DUE]}}),
        # Expiry counts
        db.vehicles.count_documents({"company_id": company_id, "rego_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        db.vehicles.count_documents({"company_id": company_id, "insurance_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        db.vehicles.count_documents({"company_id": company_id, "safety_certificate_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        db.vehicles.count_documents({"company_id": company_id, "coi_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        # Vehicle names with expiring items
        db.vehicles.find({"company_id": company_id, "rego_expiry": {"$lte": thirty_days, "$gte": today_str}}, {"name": 1, "rego_expiry": 1, "_id": 0}).to_list(10),
        db.vehicles.find({"company_id": company_id, "insurance_expiry": {"$lte": thirty_days, "$gte": today_str}}, {"name": 1, "insurance_expiry": 1, "_id": 0}).to_list(10),
        db.vehicles.find({"company_id": company_id, "coi_expiry": {"$lte": thirty_days, "$gte": today_str}}, {"name": 1, "coi_expiry": 1, "_id": 0}).to_list(10),
        # Fuel and alerts
        db.fuel_submissions.aggregate([{"$match": {"company_id": company_id, "timestamp": {"$gte": month_start}}}, {"$group": {"_id": None, "total": {"$sum": "$amount"}}}]).to_list(1),
        db.alerts.count_documents({"company_id": company_id, "is_read": False}),
        # Drivers
        db.users.find({"company_id": company_id, "role": UserRole.DRIVER}).to_list(1000),
    )
    
    # Unpack results
    total_vehicles, inspections_today, active_today, issues_today, vehicles_needing_attention, \
    upcoming_rego, upcoming_insurance, upcoming_safety_cert, upcoming_coi, \
    rego_expiring_vehicles, insurance_expiring_vehicles, coi_expiring_vehicles, \
    fuel_result, unread_alerts, drivers = results
    
    # Calculate derived values
    inspections_missed = max(0, total_vehicles - len(active_today))
    expiring_soon = upcoming_rego + upcoming_insurance + upcoming_safety_cert + upcoming_coi
    fuel_this_month = fuel_result[0]["total"] if fuel_result else 0
    
    # Process driver expiries
    drivers_license_expiring = 0
    drivers_license_expired = 0
    drivers_training_expiring = 0
    drivers_training_expired = 0
    
    for driver in drivers:
        license_exp = driver.get("license_expiry")
        if license_exp and license_exp.upper() != "NA":
            if license_exp < today_str:
                drivers_license_expired += 1
            elif license_exp <= sixty_days:
                drivers_license_expiring += 1
        
        for field in ["medical_certificate_expiry", "first_aid_expiry", "forklift_license_expiry", "dangerous_goods_expiry"]:
            exp = driver.get(field)
            if exp and exp.upper() != "NA":
                if exp < today_str:
                    drivers_training_expired += 1
                elif exp <= sixty_days:
                    drivers_training_expiring += 1
    
    result = {
        "total_vehicles": total_vehicles,
        "total_drivers": len(drivers),
        "inspections_today": inspections_today,
        "inspections_missed": inspections_missed,
        "issues_today": issues_today,
        "fuel_this_month": round(fuel_this_month, 2),
        "expiring_soon": expiring_soon,
        "active_today": len(active_today),
        "active_today_ids": active_today,
        "vehicles_needing_attention": vehicles_needing_attention,
        "upcoming_rego_expiry": upcoming_rego,
        "upcoming_insurance_expiry": upcoming_insurance,
        "upcoming_safety_cert_expiry": upcoming_safety_cert,
        "upcoming_coi_expiry": upcoming_coi,
        "rego_expiring_vehicles": rego_expiring_vehicles,
        "insurance_expiring_vehicles": insurance_expiring_vehicles,
        "coi_expiring_vehicles": coi_expiring_vehicles,
        "unread_alerts": unread_alerts,
        "drivers_license_expiring": drivers_license_expiring,
        "drivers_license_expired": drivers_license_expired,
        "drivers_training_expiring": drivers_training_expiring,
        "drivers_training_expired": drivers_training_expired,
    }
    
    # Cache the result
    set_cached_stats(company_id, result)
    
    return result


@api_router.get("/dashboard/chart-data")
async def get_dashboard_chart_data(
    current_user: dict = Depends(get_current_user),
    days: int = 7
):
    """Get weekly inspection and issue data for dashboard charts"""
    company_id = current_user["company_id"]
    
    # Limit to reasonable range
    days = min(max(days, 7), 30)
    
    # Get data for the past N days
    chart_data = []
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    for i in range(days - 1, -1, -1):
        day_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=i)
        day_end = day_start + timedelta(days=1)
        
        # Count inspections for this day
        inspections = await db.inspections.count_documents({
            "company_id": company_id,
            "timestamp": {"$gte": day_start, "$lt": day_end}
        })
        
        # Count issues (unsafe inspections) for this day
        issues = await db.inspections.count_documents({
            "company_id": company_id,
            "timestamp": {"$gte": day_start, "$lt": day_end},
            "is_safe": False
        })
        
        # Count fuel submissions for this day
        fuel_pipeline = [
            {"$match": {"company_id": company_id, "timestamp": {"$gte": day_start, "$lt": day_end}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        fuel_result = await db.fuel_submissions.aggregate(fuel_pipeline).to_list(1)
        fuel = round(fuel_result[0]["total"], 2) if fuel_result else 0
        
        chart_data.append({
            "day": day_names[day_start.weekday()],
            "date": day_start.strftime("%Y-%m-%d"),
            "inspections": inspections,
            "issues": issues,
            "fuel": fuel
        })
    
    return chart_data



# ============== Audit Trail ==============

@api_router.get("/audit-trail")
async def get_audit_trail(
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Super Admin can view audit trail")
    
    # Get all user IDs in company
    company_users = await db.users.distinct("_id", {"company_id": current_user["company_id"]})
    user_ids = [str(uid) for uid in company_users]
    
    query = {"user_id": {"$in": user_ids}}
    if entity_type:
        query["entity_type"] = entity_type
    if entity_id:
        query["entity_id"] = entity_id
    
    trail = await db.audit_trail.find(query).sort("timestamp", -1).to_list(1000)
    return serialize_doc(trail)

# ============== Subscription (Future Ready) ==============

# Company Registration with Stripe
@api_router.post("/auth/register-company")
async def register_company(data: CompanyRegister):
    """Register a new company and admin user, optionally create Stripe checkout session"""
    
    # Check if email already exists
    existing_user = await db.users.find_one({"email": data.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create company
    company_doc = {
        "name": data.company_name,
        "vehicle_count": data.vehicle_count,
        "subscription_status": "trialing",
        "subscription_plan": "pro",
        "trial_end": (datetime.utcnow() + timedelta(days=PRICING["trial_days"])).isoformat(),
        "stripe_customer_id": None,
        "stripe_subscription_id": None,
        "created_at": datetime.utcnow().isoformat(),
    }
    company_result = await db.companies.insert_one(company_doc)
    company_id = str(company_result.inserted_id)
    
    # Create admin user
    # Determine role: super_admin for Company Owner, admin for Admin (default to super_admin if not specified)
    user_role = data.role if data.role in [UserRole.SUPER_ADMIN, UserRole.ADMIN] else UserRole.SUPER_ADMIN
    
    user_doc = {
        "email": data.email,
        "password_hash": get_password_hash(data.password),
        "name": data.name,
        "role": user_role,
        "company_id": company_id,
        "created_at": datetime.utcnow().isoformat(),
    }
    user_result = await db.users.insert_one(user_doc)
    user_id = str(user_result.inserted_id)
    
    # If Stripe is configured, create checkout session
    checkout_url = None
    if stripe.api_key and data.origin_url:
        try:
            # Create Stripe customer
            customer = stripe.Customer.create(
                email=data.email,
                name=data.name,
                metadata={
                    "company_id": company_id,
                    "company_name": data.company_name,
                }
            )
            
            # Update company with Stripe customer ID
            await db.companies.update_one(
                {"_id": company_result.inserted_id},
                {"$set": {"stripe_customer_id": customer.id}}
            )
            
            # Calculate price
            total_monthly = PRICING["base_price"] + (data.vehicle_count * PRICING["per_vehicle"])
            
            # Create checkout session for subscription
            checkout_session = stripe.checkout.Session.create(
                customer=customer.id,
                payment_method_types=["card"],
                line_items=[{
                    "price_data": {
                        "currency": "aud",
                        "product_data": {
                            "name": f"FleetShield365 Pro - {data.vehicle_count} vehicles",
                            "description": f"Base ${PRICING['base_price']}/mo + ${PRICING['per_vehicle']}/vehicle",
                        },
                        "unit_amount": total_monthly * 100,  # Stripe uses cents
                        "recurring": {"interval": "month"},
                    },
                    "quantity": 1,
                }],
                mode="subscription",
                success_url=f"{data.origin_url}/payment/success?session_id={{CHECKOUT_SESSION_ID}}",
                cancel_url=f"{data.origin_url}/pricing",
                subscription_data={
                    "trial_period_days": PRICING["trial_days"],
                    "metadata": {
                        "company_id": company_id,
                        "vehicle_count": str(data.vehicle_count),
                    }
                },
            )
            checkout_url = checkout_session.url
        except Exception as e:
            logger.error(f"Stripe error: {e}")
            # Continue without Stripe - trial mode
    
    # Generate access token
    access_token = create_access_token(data={"sub": user_id})
    
    return {
        "access_token": access_token,
        "checkout_url": checkout_url,
        "company_id": company_id,
        "user_id": user_id,
    }

# Get current user with company info (for website)
@api_router.get("/auth/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Get current user and company information"""
    company = None
    if current_user.get("company_id"):
        company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
        if company:
            # Count vehicles
            vehicle_count = await db.vehicles.count_documents({"company_id": current_user["company_id"]})
            company = serialize_doc(company)
            company["vehicle_count"] = vehicle_count
    
    user_data = {
        "id": current_user["id"],
        "email": current_user["email"],
        "name": current_user["name"],
        "role": current_user.get("role", "driver"),
        "company_name": company["name"] if company else None,
    }
    
    return {
        "user": user_data,
        "company": company,
    }

# Stripe Webhook Handler
@api_router.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events"""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")
    
    if webhook_secret and sig_header:
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, webhook_secret
            )
        except Exception as e:
            logger.error(f"Webhook signature verification failed: {e}")
            raise HTTPException(status_code=400, detail="Invalid signature")
    else:
        # For testing without webhook secret
        event = json.loads(payload)
    
    event_type = event.get("type")
    data = event.get("data", {}).get("object", {})
    
    if event_type == "checkout.session.completed":
        # Payment successful, activate subscription
        company_id = data.get("metadata", {}).get("company_id")
        if company_id:
            await db.companies.update_one(
                {"_id": ObjectId(company_id)},
                {"$set": {
                    "subscription_status": "active",
                    "stripe_subscription_id": data.get("subscription"),
                }}
            )
    
    elif event_type == "customer.subscription.updated":
        # Subscription updated
        subscription_id = data.get("id")
        status = data.get("status")
        
        await db.companies.update_one(
            {"stripe_subscription_id": subscription_id},
            {"$set": {"subscription_status": status}}
        )
    
    elif event_type == "customer.subscription.deleted":
        # Subscription cancelled
        subscription_id = data.get("id")
        
        await db.companies.update_one(
            {"stripe_subscription_id": subscription_id},
            {"$set": {"subscription_status": "cancelled"}}
        )
    
    elif event_type == "invoice.payment_failed":
        # Payment failed
        subscription_id = data.get("subscription")
        
        await db.companies.update_one(
            {"stripe_subscription_id": subscription_id},
            {"$set": {"subscription_status": "past_due"}}
        )
    
    return {"received": True}

# ============== Push Notifications ==============

class PushTokenCreate(BaseModel):
    token: str
    platform: str = "ios"
    device_name: str = "Unknown Device"

class NotificationPreferencesUpdate(BaseModel):
    expiry_alerts: Optional[bool] = None
    issue_alerts: Optional[bool] = None
    missed_inspection_alerts: Optional[bool] = None
    daily_summary: Optional[bool] = None
    push_enabled: Optional[bool] = None
    email_enabled: Optional[bool] = None

@api_router.post("/push-tokens")
async def register_push_token(data: PushTokenCreate, current_user: dict = Depends(get_current_user)):
    """Register a push notification token for the current user"""
    # Check if token already exists
    existing = await db.push_tokens.find_one({"token": data.token})
    if existing:
        # Update existing token with new user
        await db.push_tokens.update_one(
            {"token": data.token},
            {"$set": {
                "user_id": current_user["id"],
                "company_id": current_user.get("company_id"),
                "platform": data.platform,
                "device_name": data.device_name,
                "updated_at": datetime.utcnow().isoformat(),
            }}
        )
    else:
        # Create new token
        await db.push_tokens.insert_one({
            "token": data.token,
            "user_id": current_user["id"],
            "company_id": current_user.get("company_id"),
            "platform": data.platform,
            "device_name": data.device_name,
            "created_at": datetime.utcnow().isoformat(),
        })
    
    return {"status": "registered"}

@api_router.delete("/push-tokens")
async def unregister_push_token(data: dict, current_user: dict = Depends(get_current_user)):
    """Unregister a push notification token"""
    token = data.get("token")
    if token:
        await db.push_tokens.delete_one({"token": token, "user_id": current_user["id"]})
    return {"status": "unregistered"}

@api_router.get("/notification-preferences")
async def get_notification_preferences(current_user: dict = Depends(get_current_user)):
    """Get notification preferences for the current user"""
    prefs = await db.notification_preferences.find_one({"user_id": current_user["id"]})
    
    if not prefs:
        # Return defaults
        return {
            "expiry_alerts": True,
            "issue_alerts": True,
            "missed_inspection_alerts": True,
            "daily_summary": False,
            "push_enabled": True,
            "email_enabled": True,
        }
    
    return {
        "expiry_alerts": prefs.get("expiry_alerts", True),
        "issue_alerts": prefs.get("issue_alerts", True),
        "missed_inspection_alerts": prefs.get("missed_inspection_alerts", True),
        "daily_summary": prefs.get("daily_summary", False),
        "push_enabled": prefs.get("push_enabled", True),
        "email_enabled": prefs.get("email_enabled", True),
    }

@api_router.put("/notification-preferences")
async def update_notification_preferences(
    data: NotificationPreferencesUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update notification preferences for the current user"""
    update_data = {k: v for k, v in data.dict().items() if v is not None}
    update_data["updated_at"] = datetime.utcnow().isoformat()
    
    await db.notification_preferences.update_one(
        {"user_id": current_user["id"]},
        {"$set": update_data},
        upsert=True
    )
    
    return {"status": "updated"}

# Push notification sender helper
async def send_push_notification(user_ids: list, title: str, body: str, data: dict = None):
    """Send push notification to specific users via Expo Push Service"""
    import httpx
    
    # Get push tokens for these users
    tokens = await db.push_tokens.find({"user_id": {"$in": user_ids}}).to_list(100)
    
    if not tokens:
        logger.info(f"[Push] No tokens found for users: {user_ids}")
        return
    
    # Check user preferences
    messages = []
    for token_doc in tokens:
        # Check if user has push enabled
        prefs = await db.notification_preferences.find_one({"user_id": token_doc["user_id"]})
        if prefs and not prefs.get("push_enabled", True):
            continue
        
        messages.append({
            "to": token_doc["token"],
            "title": title,
            "body": body,
            "data": data or {},
            "sound": "default",
            "priority": "high",
        })
    
    if not messages:
        logger.info("[Push] No messages to send (all users have push disabled)")
        return
    
    # Send to Expo Push Service
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://exp.host/--/api/v2/push/send",
                json=messages,
                headers={"Content-Type": "application/json"},
            )
            logger.info(f"[Push] Sent {len(messages)} notifications: {response.status_code}")
    except Exception as e:
        logger.error(f"[Push] Failed to send notifications: {e}")

# Helper to send alert with both email and push
async def send_alert_notification(
    alert_type: str,
    title: str,
    message: str,
    user_ids: list,
    company_id: str,
    data: dict = None
):
    """Send alert via both email and push notification based on preferences"""
    
    for user_id in user_ids:
        prefs = await db.notification_preferences.find_one({"user_id": user_id})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True}
        
        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if not user:
            continue
        
        # Check alert type preferences
        type_enabled = True
        if alert_type == "expiry" and not prefs.get("expiry_alerts", True):
            type_enabled = False
        elif alert_type == "issue" and not prefs.get("issue_alerts", True):
            type_enabled = False
        elif alert_type == "missed" and not prefs.get("missed_inspection_alerts", True):
            type_enabled = False
        elif alert_type == "daily_summary" and not prefs.get("daily_summary", False):
            type_enabled = False
        
        if not type_enabled:
            continue
        
        # Send push notification
        if prefs.get("push_enabled", True):
            await send_push_notification([user_id], title, message, data)
        
        # Send email
        if prefs.get("email_enabled", True):
            await EmailService.send_email(
                to_email=user["email"],
                subject=f"FleetShield365 Alert: {title}",
                body=f"<h2>{title}</h2><p>{message}</p>",
                company_id=company_id,
                is_html=True
            )

@api_router.get("/subscription")
async def get_subscription(current_user: dict = Depends(get_current_user)):
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    
    plans = {
        "basic": {"max_vehicles": 5, "price": 0},
        "standard": {"max_vehicles": 20, "price": 49},
        "pro": {"max_vehicles": float("inf"), "price": 99}
    }
    
    current_plan = company.get("subscription_plan", "basic")
    plan_details = plans.get(current_plan, plans["basic"])
    
    return {
        "current_plan": current_plan,
        "plan_details": plan_details,
        "active_vehicles": company.get("active_vehicles_count", 0),
        "billing_history": company.get("billing_history", [])
    }


# ============== Support Requests ==============

@api_router.post("/support")
async def create_support_request(
    request_data: SupportRequestCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create a new support request"""
    support_request = {
        "_id": ObjectId(),
        "company_id": current_user["company_id"],
        "user_id": str(current_user["_id"]),
        "user_name": current_user.get("name", "Unknown"),
        "user_email": current_user.get("email", ""),
        "user_role": current_user.get("role", "driver"),
        "subject": request_data.subject,
        "message": request_data.message,
        "category": request_data.category,
        "status": SupportRequestStatus.OPEN,
        "admin_response": None,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "resolved_at": None,
    }
    
    await db.support_requests.insert_one(support_request)
    
    return {
        "id": str(support_request["_id"]),
        "message": "Support request submitted successfully. We'll get back to you soon!",
        "ticket_number": f"SR-{str(support_request['_id'])[-6:].upper()}"
    }

@api_router.get("/support")
async def get_support_requests(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = None,
    limit: int = 50
):
    """Get support requests - admins see all for company, users see their own"""
    query = {"company_id": current_user["company_id"]}
    
    # Non-admins only see their own requests
    if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        query["user_id"] = str(current_user["_id"])
    
    if status:
        query["status"] = status
    
    requests = await db.support_requests.find(query).sort("created_at", -1).limit(limit).to_list(limit)
    
    return [{
        "id": str(r["_id"]),
        "ticket_number": f"SR-{str(r['_id'])[-6:].upper()}",
        "user_name": r.get("user_name"),
        "user_email": r.get("user_email"),
        "user_role": r.get("user_role"),
        "subject": r.get("subject"),
        "message": r.get("message"),
        "category": r.get("category"),
        "status": r.get("status"),
        "admin_response": r.get("admin_response"),
        "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
        "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None,
        "resolved_at": r.get("resolved_at").isoformat() if r.get("resolved_at") else None,
    } for r in requests]

@api_router.get("/support/stats")
async def get_support_stats(
    current_user: dict = Depends(get_current_user)
):
    """Get support request stats for admins"""
    if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    company_id = current_user["company_id"]
    
    total = await db.support_requests.count_documents({"company_id": company_id})
    open_count = await db.support_requests.count_documents({"company_id": company_id, "status": "open"})
    in_progress = await db.support_requests.count_documents({"company_id": company_id, "status": "in_progress"})
    resolved = await db.support_requests.count_documents({"company_id": company_id, "status": "resolved"})
    
    return {
        "total": total,
        "open": open_count,
        "in_progress": in_progress,
        "resolved": resolved,
    }

@api_router.get("/support/{request_id}")
async def get_support_request(
    request_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get a single support request"""
    request = await db.support_requests.find_one({"_id": ObjectId(request_id)})
    
    if not request:
        raise HTTPException(status_code=404, detail="Support request not found")
    
    # Check access
    if request["company_id"] != current_user["company_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        if request["user_id"] != str(current_user["_id"]):
            raise HTTPException(status_code=403, detail="Access denied")
    
    return {
        "id": str(request["_id"]),
        "ticket_number": f"SR-{str(request['_id'])[-6:].upper()}",
        "user_name": request.get("user_name"),
        "user_email": request.get("user_email"),
        "user_role": request.get("user_role"),
        "subject": request.get("subject"),
        "message": request.get("message"),
        "category": request.get("category"),
        "status": request.get("status"),
        "admin_response": request.get("admin_response"),
        "created_at": request.get("created_at").isoformat() if request.get("created_at") else None,
        "updated_at": request.get("updated_at").isoformat() if request.get("updated_at") else None,
        "resolved_at": request.get("resolved_at").isoformat() if request.get("resolved_at") else None,
    }

@api_router.put("/support/{request_id}")
async def update_support_request(
    request_id: str,
    update_data: SupportRequestUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update support request (admin only) - respond or change status"""
    if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    request = await db.support_requests.find_one({"_id": ObjectId(request_id)})
    
    if not request:
        raise HTTPException(status_code=404, detail="Support request not found")
    
    if request["company_id"] != current_user["company_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    update_fields = {"updated_at": datetime.utcnow()}
    
    if update_data.status:
        update_fields["status"] = update_data.status
        if update_data.status in [SupportRequestStatus.RESOLVED, SupportRequestStatus.CLOSED]:
            update_fields["resolved_at"] = datetime.utcnow()
    
    if update_data.admin_response:
        update_fields["admin_response"] = update_data.admin_response
    
    await db.support_requests.update_one(
        {"_id": ObjectId(request_id)},
        {"$set": update_fields}
    )
    
    return {"message": "Support request updated successfully"}

# FAQ Data (static, no database needed)
FAQ_DATA = [
    {
        "category": "driver",
        "questions": [
            {
                "q": "How do I complete a pre-start inspection?",
                "a": "1. Open the app and select your vehicle from the dropdown\n2. Tap 'START PRESTART INSPECTION'\n3. Go through each checklist item and mark as OK or Not OK\n4. Add photos of any issues found\n5. Sign at the bottom and submit"
            },
            {
                "q": "What do I do if I find an issue during inspection?",
                "a": "Mark the item as 'Not OK', add a description of the issue, and take a photo. Your admin will be notified automatically. If the vehicle is unsafe to drive, do not operate it until the issue is resolved."
            },
            {
                "q": "How do I submit a fuel receipt?",
                "a": "1. Tap 'FUEL SUBMISSION' on the home screen\n2. Select the vehicle you fueled\n3. Enter the fuel amount, cost, and odometer reading\n4. Take a photo of the receipt\n5. Submit"
            },
            {
                "q": "Can I use the app without internet?",
                "a": "Yes! The app works offline. Your inspections, fuel submissions, and incident reports will be saved locally and automatically sync when you have internet again. You'll see a 'Pending Sync' indicator."
            },
            {
                "q": "How do I report an incident or accident?",
                "a": "1. Tap 'INCIDENT REPORT' (red button) on the home screen\n2. If someone is injured, tap the emergency banner to call 000\n3. Fill in the incident details, other party information, and take photos\n4. Submit the report - your admin will be notified"
            },
            {
                "q": "Where can I see my past inspections?",
                "a": "Your admin can view all inspection history in the Reports section of the admin website. As a driver, you can see your recent activity on the app's home screen."
            }
        ]
    },
    {
        "category": "admin",
        "questions": [
            {
                "q": "How do I add a new vehicle?",
                "a": "1. Go to Vehicles page\n2. Click '+ Add Vehicle'\n3. Fill in vehicle details (name, rego, type)\n4. Add expiry dates for registration, insurance, etc.\n5. Save"
            },
            {
                "q": "How do I add a new driver?",
                "a": "1. Go to Drivers page\n2. Click '+ Add Driver'\n3. Enter driver details and create login credentials\n4. Add license and certification expiry dates\n5. Click 'Send Login' to email their credentials"
            },
            {
                "q": "What are expiry alerts?",
                "a": "The system automatically monitors all expiry dates (vehicle rego, insurance, driver licenses, etc.) and sends alerts at 60, 30, 14, and 7 days before expiry. Critical items (7 days or less) appear in red."
            },
            {
                "q": "How do I view inspection reports?",
                "a": "Go to the Reports page. You can filter by date, vehicle, or inspection type. Click 'View Details' on any report to see the full inspection including photos and signatures."
            },
            {
                "q": "How do I assign drivers to vehicles?",
                "a": "Go to Vehicles page, find the vehicle, and click 'Assign'. Select one or more drivers who are authorized to operate that vehicle."
            },
            {
                "q": "How do I change my company logo?",
                "a": "Go to Settings > General tab. Click on the logo area to upload your company logo. This logo will appear on PDF reports and in the app."
            }
        ]
    },
    {
        "category": "general",
        "questions": [
            {
                "q": "Is my data secure?",
                "a": "Yes. All data is encrypted in transit (HTTPS) and at rest. We use industry-standard security practices and your data is never shared with third parties."
            },
            {
                "q": "How do I reset my password?",
                "a": "Contact your company admin to reset your password, or use the 'Forgot Password' link on the login screen."
            },
            {
                "q": "What devices does the app work on?",
                "a": "The driver app works on iOS and Android phones. The admin website works on any modern web browser (Chrome, Safari, Firefox, Edge)."
            }
        ]
    }
]

@api_router.get("/faq")
async def get_faq():
    """Get FAQ data - no auth required"""
    return FAQ_DATA


# ============== Health Check ==============

@api_router.get("/")
async def root():
    return {"message": "FleetShield365 API", "version": "1.0.0"}

@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Include router
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    # Create indexes for better query performance
    # Drop old email index and create a sparse one (allows multiple nulls)
    try:
        await db.users.drop_index("email_1")
    except:
        pass  # Index might not exist
    try:
        await db.users.drop_index("company_id_1_username_1")
    except:
        pass  # Index might not exist
    
    # Sparse indexes allow multiple null values
    await db.users.create_index("email", unique=True, sparse=True)
    await db.users.create_index("username", sparse=True)  # Not unique globally, just for lookups
    await db.users.create_index([("company_id", 1), ("role", 1)])
    await db.vehicles.create_index([("company_id", 1), ("registration_number", 1)])
    await db.vehicles.create_index([("company_id", 1), ("status", 1)])
    await db.inspections.create_index([("company_id", 1), ("timestamp", -1)])
    await db.inspections.create_index([("company_id", 1), ("vehicle_id", 1), ("timestamp", -1)])
    await db.inspections.create_index([("driver_id", 1), ("timestamp", -1)])
    await db.inspection_photos.create_index([("vehicle_id", 1), ("created_at", -1)])
    await db.inspection_photos.create_index("inspection_id")
    await db.alerts.create_index([("company_id", 1), ("is_read", 1)])
    await db.alerts.create_index([("company_id", 1), ("created_at", -1)])
    await db.maintenance_logs.create_index([("company_id", 1), ("service_date", -1)])
    await db.fuel_submissions.create_index([("company_id", 1), ("timestamp", -1)])
    # Indexes for expiry date queries (dashboard performance)
    await db.vehicles.create_index([("company_id", 1), ("rego_expiry", 1)])
    await db.vehicles.create_index([("company_id", 1), ("insurance_expiry", 1)])
    await db.vehicles.create_index([("company_id", 1), ("safety_certificate_expiry", 1)])
    await db.vehicles.create_index([("company_id", 1), ("coi_expiry", 1)])
    logger.info("Database indexes created")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
