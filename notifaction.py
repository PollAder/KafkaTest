from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('user-verification',
                         bootstrap_servers='localhost:9092',
                         group_id='notification-group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def send_verification_email(email, code):
    print(f'Sending verification email to {email} with code {code}')

for message in consumer:
    data = message.value
    email = data['email']
    code = data['code']
    
    # Логика отправки кода подтверждения на email
    send_verification_email(email, code)