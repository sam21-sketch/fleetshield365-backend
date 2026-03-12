"""
PDF generation service for inspection reports
"""
import base64
import logging
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
from reportlab.lib.units import inch

logger = logging.getLogger(__name__)


async def generate_inspection_pdf(inspection: dict, vehicle: dict, driver: dict, company: dict) -> str:
    """Generate PDF report for inspection and return base64 encoded string"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#1a365d'), spaceAfter=12)
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#2d3748'), spaceAfter=8)
    normal_style = ParagraphStyle('Normal', parent=styles['Normal'], fontSize=10, spaceAfter=6)
    
    if company and company.get('logo_base64'):
        try:
            logo_data = base64.b64decode(company['logo_base64'].split(',')[-1] if ',' in company['logo_base64'] else company['logo_base64'])
            logo_img = RLImage(BytesIO(logo_data), width=2*inch, height=1*inch)
            elements.append(logo_img)
            elements.append(Spacer(1, 12))
        except:
            pass
    
    inspection_type = "Prestart Inspection Report" if inspection['type'] == 'prestart' else "End of Shift Report"
    elements.append(Paragraph(inspection_type, title_style))
    elements.append(Spacer(1, 12))
    
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
    
    if inspection['type'] == 'prestart' and inspection.get('checklist_items'):
        elements.append(Paragraph("Inspection Checklist", heading_style))
        
        checklist_data = [['Item', 'Section', 'Status', 'Comment']]
        for item in inspection['checklist_items']:
            status_color = 'OK' if item['status'] == 'ok' else ('ISSUE' if item['status'] == 'issue' else 'N/A')
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
    
    if inspection.get('photos') and len(inspection['photos']) > 0:
        elements.append(Paragraph("Inspection Photos", heading_style))
        elements.append(Spacer(1, 10))
        
        for i, photo in enumerate(inspection['photos'][:6]):
            try:
                photo_base64 = photo.get('base64_data', '')
                if photo_base64:
                    if ',' in photo_base64:
                        photo_base64 = photo_base64.split(',')[-1]
                    
                    photo_bytes = base64.b64decode(photo_base64)
                    photo_img = RLImage(BytesIO(photo_bytes), width=3*inch, height=2.5*inch)
                    
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
    
    elements.append(Paragraph("Declaration", heading_style))
    if inspection['type'] == 'prestart':
        declaration_text = "I confirm this vehicle is safe to operate."
    else:
        declaration_text = "I confirm this report is accurate."
    elements.append(Paragraph(f"Confirmed: {declaration_text}", normal_style))
    
    elements.append(Spacer(1, 30))
    footer_text = f"Generated by FleetShield365 | Report ID: {str(inspection.get('_id', 'N/A'))[:8]}"
    elements.append(Paragraph(footer_text, ParagraphStyle('Footer', fontSize=8, textColor=colors.gray)))
    
    doc.build(elements)
    pdf_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    return pdf_base64
