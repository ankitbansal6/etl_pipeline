import smtplib
from email.message import EmailMessage

#generate password 
#https://myaccount.google.com/apppasswords
def send_email_native(subject, body, sender, password, recipient):
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)
    print("Email Sent")


