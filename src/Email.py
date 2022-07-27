import smtplib


class Email_Class:
    def __init__(self, email_server_name, email_port):
        self.smtp_server = smtplib.SMTP(email_server_name, email_port)

    def send_mail(self, message, recipient):
        self.smtp_server.sendmail(
            "ServiceNowToCollibraPipeline",
            recipient,
            message,
        )
