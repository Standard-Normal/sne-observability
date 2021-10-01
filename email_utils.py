from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os
import pandas as pd


EMAILS = ['c.girabawe@standardnormal.com']


def get_password(email_address, credentials_path='gmail_credentials.json'):
    creds = json.load(open(credentials_path, 'r'))
    password = creds[email_address]['password']
    return password


def send_mail(body, to_email, subject):
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = 'stdnormaldev@gmail.com'
    message['To'] = to_email

    body_content = body
    message.attach(MIMEText(body_content, "html"))
    msg_body = message.as_string()

    server = SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(message['From'], get_password(message['From']))
    server.sendmail(message['From'], message['To'], msg_body)
    server.quit()


def send_flag_report(to_email, body, subject='[FLAG] Data Incomplete'):
    assert isinstance(to_email, str)
    send_mail(body, to_email, subject)
    print(f"Mail Sent To {to_email}")

if __name__ == '__main__':
    d = {'test': 'value', 'msg': ['a', 'b', [1, 2]]}
    body = f"""
    {d}
    """
    send_flag_report('c.girabawe@standardnormal.com', body)
