"""Notifikationsmetoder til de fundne fejl, slået op på trigger-argumentet notification_type"""

from notifikationer.mail.sender import handle_email
from notifikationer.servicenow.sender import handle_servicenow

NOTIFICATION_HANDLERS = {
    "Send mail": handle_email,
    "Opret ServiceNow-sag": handle_servicenow,
}
