"""
Email notification service using SendGrid
"""
import os
import logging
from datetime import datetime
from typing import List
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from utils.database import db

logger = logging.getLogger(__name__)

# SendGrid Configuration
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'noreply@fleetguard.app')


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


async def send_issue_alert_email(admin_email: str, company_name: str, vehicle_name: str, driver_name: str, issue_summary: str, inspection_type: str, photos: List[dict] = None, inspection_id: str = None):
    """Send issue alert email when an inspection has issues - WITH PHOTOS"""
    
    photo_html = ""
    if photos and len(photos) > 0:
        photo_html = """
        <div style="margin: 20px 0;">
            <h3 style="color: #374151;">Inspection Photos:</h3>
            <div style="display: flex; flex-wrap: wrap; gap: 10px;">
        """
        for photo in photos[:8]:
            photo_type = photo.get('photo_type', 'Photo').replace('_', ' ').title()
            base64_data = photo.get('base64_data', '')
            if base64_data:
                if not base64_data.startswith('data:'):
                    base64_data = f"data:image/jpeg;base64,{base64_data}"
                photo_html += f"""
                <div style="text-align: center;">
                    <img src="{base64_data}" style="width: 150px; height: 120px; object-fit: cover; border-radius: 8px; border: 2px solid {'#DC2626' if 'damage' in photo_type.lower() else '#E5E7EB'};" />
                    <p style="font-size: 11px; color: #6B7280; margin: 4px 0;">{photo_type}</p>
                </div>
                """
        photo_html += "</div></div>"
    
    dashboard_link = f"https://www.fleetshield365.com/dashboard"
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #DC2626; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">DEFECT ALERT - Immediate Attention Required</h2>
        </div>
        
        <div style="border: 1px solid #E5E7EB; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
            <p>Hi {company_name} Admin,</p>
            <p><strong>A defect has been reported and requires your immediate attention:</strong></p>
            
            <div style="background-color: #FEF2F2; border-left: 4px solid #DC2626; padding: 16px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 8px 0; color: #6B7280;">Vehicle:</td><td style="padding: 8px 0; font-weight: bold;">{vehicle_name}</td></tr>
                    <tr><td style="padding: 8px 0; color: #6B7280;">Driver:</td><td style="padding: 8px 0;">{driver_name}</td></tr>
                    <tr><td style="padding: 8px 0; color: #6B7280;">Inspection Type:</td><td style="padding: 8px 0;">{inspection_type}</td></tr>
                    <tr><td style="padding: 8px 0; color: #6B7280;">Time:</td><td style="padding: 8px 0;">{datetime.utcnow().strftime('%I:%M %p, %B %d, %Y')}</td></tr>
                </table>
                <hr style="border: none; border-top: 1px solid #FECACA; margin: 15px 0;" />
                <p style="color: #DC2626; font-weight: bold; margin: 0;">Issue Reported:</p>
                <p style="color: #991B1B; margin: 8px 0 0 0;">{issue_summary}</p>
            </div>
            
            {photo_html}
            
            <div style="margin-top: 25px; text-align: center;">
                <a href="{dashboard_link}" style="background-color: #0891B2; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">View Full Inspection Report</a>
            </div>
            
            <p style="color: #9CA3AF; font-size: 12px; margin-top: 30px; text-align: center;">
                This is an automated alert from FleetShield365.<br/>
                Vehicle may need to be taken off road pending inspection.
            </p>
        </div>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[DEFECT ALERT] {vehicle_name} - {issue_summary[:50]}", html_content)


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


class EmailService:
    """SendGrid email service for sending notifications"""
    
    @staticmethod
    async def send_email(to_email: str, subject: str, body: str, company_id: str = None, is_html: bool = True):
        """Send email via SendGrid (if configured) or fallback to logging"""
        sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
        sender_email = os.environ.get('SENDER_EMAIL', 'noreply@fleetguard.app')
        
        email_log = {
            "to_email": to_email,
            "subject": subject,
            "body": body[:500],
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
            "unsafe_vehicle": "URGENT: Vehicle Marked Unsafe",
            "repeated_issues": "Alert: Repeated Vehicle Issues",
            "expiry_warning": "Reminder: Upcoming Vehicle Expiry",
            "expiry_critical": "CRITICAL: Document Has Expired",
            "driver_expiry_warning": "Reminder: Driver Document Expiring",
            "driver_expiry_critical": "CRITICAL: Driver Document Expired",
            "vehicle_offline": "Vehicle Status: Offline"
        }
        subject = subject_map.get(alert_type, "FleetShield365 Alert")
        
        alert_color = "#EF4444" if "critical" in alert_type or "unsafe" in alert_type else "#F59E0B"
        
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
