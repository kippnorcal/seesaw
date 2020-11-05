import os
import smtplib
import ssl

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Mailer:
    def __init__(self, jobname):
        self.jobname = jobname
        self.user = os.getenv("SENDER_EMAIL")
        self.password = os.getenv("SENDER_PWD")
        self.to_email = os.getenv("RECIPIENT_EMAIL")
        context = ssl.create_default_context()
        self.server = smtplib.SMTP_SSL(
            os.getenv("EMAIL_SERVER"), os.getenv("EMAIL_PORT"), context=context
        )

    def _subject_line(self):
        subject_type = "Error" if self.error_message else "Success"
        return f"{self.jobname} - {subject_type}"

    def _body_text(self):
        if self.error_message:
            return f"{self.jobname} encountered an error.\n{self.error_message}"
        else:
            return f"{self.jobname} completed successfully."

    def _attachments(self, msg):
        filename = "app.log"
        if os.path.exists(filename):
            with open(filename, "r") as attachment:
                log = MIMEText(attachment.read())
            log.add_header("Content-Disposition", f"attachment; filename= {filename}")
            msg.attach(log)

    def _message(self):
        msg = MIMEMultipart()
        msg["Subject"] = self._subject_line()
        msg["From"] = self.user
        msg["To"] = self.to_email
        msg.attach(MIMEText(self._body_text(), "plain"))
        self._attachments(msg)
        return msg.as_string()

    def notify(self, error_message=None):
        self.error_message = error_message
        with self.server as s:
            s.login(self.user, self.password)
            msg = self._message()
            s.sendmail(self.user, self.to_email, msg)
