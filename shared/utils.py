from mailersend import emails
from prefect import task
from prefect.variables import Variable

@task
def send_email(from_email: str, to_emails: list[str], subject: str, body: str):
    mailersend_credentials : dict = Variable.get("mailersend_credentials")
    mailer = emails.NewEmail(mailersend_credentials.get("api_key"))
    mail_body = {}
    mail_from = {
        "email": from_email
    }
    recipients = [ { "email": email } for email in to_emails ]
    mailer.set_mail_from(mail_from, mail_body)
    mailer.set_mail_to(recipients, mail_body)
    mailer.set_subject(subject, mail_body)
    mailer.set_html_content(body, mail_body)
    mailer.send(mail_body)