from flask import Flask, request, jsonify
from kafka import KafkaProducer
import uuid
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/register', methods=['POST'])
def register_user():
    data = request.json
    email = data.get('email')
    
    if not email:
        return jsonify({'error': 'Email is required'}), 400
    
    verification_code = str(uuid.uuid4())
    
    # Отправка кода подтверждения в Kafka
    producer.send('user-verification', {'email': email, 'code': verification_code})
    
    # Отправка приветственного письма (логика отправки письма здесь)
    send_welcome_email(email)
    
    return jsonify({'message': 'User registered successfully, verification email sent.'}), 201

def send_welcome_email(email):
    print(f'Welcome email sent to {email}')

if __name__ == '__main__':
    app.run(port=5000)