"""
Pydantic models for request/response validation
"""
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from enum import Enum


# ============== Constants/Enums ==============

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


class ServiceType(str, Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    OTHER = "other"


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


class IncidentSeverity:
    MINOR = "minor"
    MODERATE = "moderate"
    SEVERE = "severe"


# ============== Auth Models ==============

class UserRegister(BaseModel):
    email: Optional[EmailStr] = None
    password: str
    name: str
    username: Optional[str] = None
    phone: Optional[str] = None
    role: str = UserRole.DRIVER
    company_id: Optional[str] = None
    license_number: Optional[str] = None
    license_class: Optional[str] = None
    license_expiry: Optional[str] = None
    medical_certificate_expiry: Optional[str] = None
    first_aid_expiry: Optional[str] = None
    forklift_license_expiry: Optional[str] = None
    dangerous_goods_expiry: Optional[str] = None


class UserLogin(BaseModel):
    email: Optional[str] = None
    username: Optional[str] = None
    password: str
    remember_me: bool = False


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


class ForgotPasswordRequest(BaseModel):
    email: str
    origin_url: str = "https://system-monitor-33.preview.emergentagent.com"


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class AdminResetPasswordRequest(BaseModel):
    new_password: str


# ============== Company Models ==============

class CompanyCreate(BaseModel):
    name: str
    logo_base64: Optional[str] = None


class CompanyUpdate(BaseModel):
    name: Optional[str] = None
    logo_base64: Optional[str] = None
    subscription_plan: Optional[str] = None


class CompanyRegister(BaseModel):
    company_name: str
    name: str
    email: EmailStr
    password: str
    vehicle_count: int = 5
    origin_url: Optional[str] = None
    role: Optional[str] = None


# ============== User Models ==============

class UserCreate(BaseModel):
    email: EmailStr
    full_name: str
    password: str
    role: str = "admin"


class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None


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


# ============== Vehicle Models ==============

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


class DriverAssignment(BaseModel):
    driver_ids: List[str]


# ============== Inspection Models ==============

class ChecklistItem(BaseModel):
    name: str
    section: str
    status: str = ChecklistItemStatus.OK
    comment: Optional[str] = None


class InspectionPhoto(BaseModel):
    photo_type: str
    base64_data: str
    timestamp: str
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    ai_damage_status: str = AIDamageStatus.NO_DAMAGE


class DigitalAgreement(BaseModel):
    driver_name: str
    driver_id: Optional[str] = None
    agreed_at: str
    declaration_text: str
    device_info: Optional[str] = None


class PrestartCreate(BaseModel):
    vehicle_id: str
    odometer: int
    checklist_items: List[ChecklistItem]
    photos: List[InspectionPhoto]
    signature_base64: Optional[str] = None
    digital_agreement: Optional[DigitalAgreement] = None
    declaration_confirmed: bool = True
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None


class EndShiftCreate(BaseModel):
    vehicle_id: str
    odometer: int
    fuel_level: str
    new_damage: bool = False
    incident_today: bool = False
    cleanliness: str
    damage_comment: Optional[str] = None
    incident_comment: Optional[str] = None
    photos: Optional[List[InspectionPhoto]] = []
    signature_base64: Optional[str] = None
    digital_agreement: Optional[DigitalAgreement] = None
    declaration_confirmed: bool = True
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None


# ============== Fuel Models ==============

class FuelSubmission(BaseModel):
    vehicle_id: str
    amount: float
    liters: float
    receipt_photo_base64: Optional[str] = None
    odometer: Optional[int] = None
    fuel_station: Optional[str] = None
    notes: Optional[str] = None


# ============== Service Record Models ==============

class ServiceRecordCreate(BaseModel):
    vehicle_id: str
    service_date: str
    service_type: ServiceType
    service_type_other: Optional[str] = None
    description: str
    cost: Optional[float] = None
    odometer_reading: Optional[int] = None
    technician_name: Optional[str] = None
    workshop_name: Optional[str] = None
    next_service_date: Optional[str] = None
    next_service_odometer: Optional[int] = None
    attachments: Optional[List[str]] = []
    warranty_until: Optional[str] = None
    warranty_notes: Optional[str] = None


class ServiceRecordUpdate(BaseModel):
    service_date: Optional[str] = None
    service_type: Optional[ServiceType] = None
    service_type_other: Optional[str] = None
    description: Optional[str] = None
    cost: Optional[float] = None
    odometer_reading: Optional[int] = None
    technician_name: Optional[str] = None
    workshop_name: Optional[str] = None
    next_service_date: Optional[str] = None
    next_service_odometer: Optional[int] = None
    attachments: Optional[List[str]] = None
    warranty_until: Optional[str] = None
    warranty_notes: Optional[str] = None


# ============== Maintenance Models ==============

class MaintenanceLogCreate(BaseModel):
    vehicle_id: str
    service_date: str
    service_type: str
    cost: float
    workshop_name: str
    invoice_base64: Optional[str] = None
    notes: Optional[str] = None


# ============== Alert Models ==============

class AlertCreate(BaseModel):
    type: str
    message: str
    vehicle_id: Optional[str] = None
    driver_id: Optional[str] = None


# ============== Incident Models ==============

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
    severity: str = IncidentSeverity.MODERATE
    location_address: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    other_party: OtherPartyDetails
    witnesses: Optional[List[WitnessDetails]] = []
    police_report_number: Optional[str] = None
    injuries_occurred: bool = False
    injury_description: Optional[str] = None
    damage_photos: List[str] = []
    other_vehicle_photos: List[str] = []
    scene_photos: List[str] = []


class IncidentUpdate(BaseModel):
    status: Optional[str] = None
    admin_notes: Optional[str] = None
    insurance_claim_number: Optional[str] = None
    resolution_details: Optional[str] = None


# ============== Support Models ==============

class SupportRequestCreate(BaseModel):
    subject: str
    message: str
    category: SupportRequestCategory = SupportRequestCategory.GENERAL


class SupportRequestUpdate(BaseModel):
    status: Optional[SupportRequestStatus] = None
    admin_response: Optional[str] = None


# ============== Push/Notification Models ==============

class PushTokenCreate(BaseModel):
    token: str
    device_type: str = "ios"


class NotificationPreferencesUpdate(BaseModel):
    push_enabled: Optional[bool] = None
    email_enabled: Optional[bool] = None
    expiry_alerts: Optional[bool] = None
    issue_alerts: Optional[bool] = None
    missed_inspection_alerts: Optional[bool] = None
    daily_summary: Optional[bool] = None


# ============== Driver License Photo Models ==============

class LicensePhotoUpload(BaseModel):
    front_photo: Optional[str] = None
    back_photo: Optional[str] = None


class PasswordVerification(BaseModel):
    password: str


class DocumentDownloadRequest(BaseModel):
    driver_ids: List[str]


# ============== Email Test Model ==============

class TestEmailRequest(BaseModel):
    to_email: str


# ============== Pricing Configuration ==============

PRICING = {
    "base_price": 39,
    "per_vehicle": 5,
    "trial_days": 14,
}
