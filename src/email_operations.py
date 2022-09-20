import smtplib


class Email_Class:
    """ This class handles the emailing functionality primarily used for emailing the log file to the admin user"""
    def __init__(self, email_server_name, email_port):
        self.smtp_server = smtplib.SMTP(email_server_name, email_port)

    def send_mail(self, message, recipient):
        """
        Simply sends a message to the specified recepient
        :param message: The message that will be emailed, including the subject as well as the content
        :param recipient: The recipient of the message. This is specified in the config.yaml file
        """
        self.smtp_server.sendmail(
            "ServiceNowToCollibraPipeline",
            recipient,
            message,
        )
