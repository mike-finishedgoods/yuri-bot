import os
import json
import logging
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/calendar.events']
CALENDAR_ID = os.environ.get('GOOGLE_CALENDAR_ID')

_service = None


def _get_service():
    """Initialize and cache the Google Calendar service"""
    global _service
    if _service:
        return _service

    creds_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_json:
        logger.warning("⚠ GOOGLE_SERVICE_ACCOUNT_JSON not configured")
        return None

    if not CALENDAR_ID:
        logger.warning("⚠ GOOGLE_CALENDAR_ID not configured")
        return None

    try:
        creds_data = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_data, scopes=SCOPES
        )
        _service = build('calendar', 'v3', credentials=credentials)
        logger.info("✓ Connected to Google Calendar")
        return _service
    except Exception as e:
        logger.error(f"Failed to initialize Google Calendar: {str(e)}")
        return None


def create_ooo_event(user_name, first_day_off, last_day_off):
    """Create an all-day OOO event on the FGI Team Calendar.
    
    Args:
        user_name: Display name of the employee
        first_day_off: Start date as 'YYYY-MM-DD' string
        last_day_off: End date as 'YYYY-MM-DD' string
    
    Returns:
        dict with success status and event details, or error
    """
    service = _get_service()
    if not service:
        logger.warning("Google Calendar not configured — skipping event creation")
        return {"success": False, "error": "Google Calendar not configured"}

    try:
        # Google Calendar all-day events use exclusive end date
        # So if someone is out Feb 20-21, end date needs to be Feb 22
        end_date = datetime.strptime(last_day_off, '%Y-%m-%d') + timedelta(days=1)
        end_date_str = end_date.strftime('%Y-%m-%d')

        event = {
            'summary': f'{user_name} - Out of Office',
            'eventType': 'default',
            'start': {
                'date': first_day_off,
            },
            'end': {
                'date': end_date_str,
            },
            'transparency': 'transparent',  # Shows as "free" so it doesn't block calendars
        }

        created_event = service.events().insert(
            calendarId=CALENDAR_ID,
            body=event
        ).execute()

        logger.info(f"✓ Calendar event created: {created_event.get('htmlLink')}")
        return {
            "success": True,
            "event_id": created_event.get('id'),
            "link": created_event.get('htmlLink')
        }

    except Exception as e:
        logger.error(f"Failed to create calendar event: {str(e)}")
        return {"success": False, "error": str(e)}
