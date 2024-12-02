import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# SMTP server and port (replace with your server details)
smtp_server = "your_smtp_server_address"
smtp_port = 25  # Default port for non-secure SMTP communication

# Sender and recipient
sender_email = "your_email@example.com"
recipient_email = "recipient_email@example.com"

# Email content
subject = "Test Email"
body = "This is a test email sent using Python with Windows authentication."

# Create the email
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = recipient_email
message["Subject"] = subject

# Attach the email body
message.attach(MIMEText(body, "plain"))

try:
    # Connect to the SMTP server
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        # No login required for Windows authentication
        server.sendmail(sender_email, recipient_email, message.as_string())
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")
