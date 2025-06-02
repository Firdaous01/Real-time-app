import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

load_dotenv()

def send_email():
    sender_email = os.getenv("EMAIL_SENDER")
    receiver_email = os.getenv("EMAIL_RECEIVER")
    # Password not needed for MailDev, but keeping for compatibility
    password = os.getenv("EMAIL_PASSWORD")
    subject = os.getenv("EMAIL_SUBJECT")
    body_template = os.getenv("EMAIL_BODY")
    dashboard_link = os.getenv("DASHBOARD_LINK")
    
    body = body_template.format(link=dashboard_link)
    
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    
    message.attach(MIMEText(body, "plain"))
    
    try:
        # Connect to MailDev SMTP server instead of Gmail
        # Use localhost:1025 if running outside Docker
        # Use mail-dev:1025 if running inside Docker container
        server = smtplib.SMTP("localhost", 1025)
        
        # MailDev doesn't require authentication or TLS
        # server.starttls()  # Not needed for MailDev
        # server.login(sender_email, password)  # Not needed for MailDev
        
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.quit()
        
        print("Email sent successfully to MailDev!")
        print("Check the MailDev web interface at http://localhost:1080")
        
    except Exception as e:
        print(f"Error sending email: {e}")

# Alternative function for when running outside Docker
def send_email_localhost():
    sender_email = os.getenv("EMAIL_SENDER")
    receiver_email = os.getenv("EMAIL_RECEIVER")
    subject = os.getenv("EMAIL_SUBJECT")
    body_template = os.getenv("EMAIL_BODY")
    dashboard_link = os.getenv("DASHBOARD_LINK")
    
    body = body_template.format(link=dashboard_link)
    
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    
    message.attach(MIMEText(body, "plain"))
    
    try:
        # Use localhost when running outside Docker
        server = smtplib.SMTP("localhost", 1025)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.quit()
        
        print("Email sent successfully to MailDev!")
        print("Check the MailDev web interface at http://localhost:1080")
        
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    send_email()