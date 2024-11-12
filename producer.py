import time
import json
from kafka import KafkaProducer
from faker import Faker
import random

# Initialize Faker and Kafka producer
fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9094'],
                        api_version=(0, 10),
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

# Kafka topic name
topic = 'cc_retail_data'

# Fake Credit Card data
def generate_purchase():
    return {
        'reference': fake.uuid4(),
        'orderId': fake.uuid4(),
        'shopperInteraction': random.choice(["ecommerce", "instore", "subscription"]),
        'transactionId': fake.uuid4(),
        'paymentId': fake.uuid4(),
        'paymentMethod': random.choice(["Diners", "Visa", "Mastercard","American Express"]),
        'paymentMethodCustomCode': "null",
        'merchantName': fake.name(),
        'card': {
            'holder': fake.name(),
            'number': fake.credit_card_number(),
            'csc': "***",
            'expiration': {
                'month': random.randint(1, 12),
                'year': random.randint(2024, 2034)
            },
            'document': random.randint(10**11, 10**12),
            'token': "null"
        },
        'value': round(random.uniform(10.0, 100.0), 2),
        'referenceValue': round(random.uniform(10.0, 100.0), 2),
        'currency': random.choice(["BRL", "USD", "EUR"]),
        'installments': 1,
        'installmentsInterestRate': 0,
        'installmentsValue': round(random.uniform(10.0, 100.0), 2),
        'deviceFingerprint': random.randint(10**8, 10**9),
        'ipAddress': fake.ipv4(),

        "miniCart": {
            "buyer": {
                'id': fake.uuid4(),
                'firstName': fake.name(),
                'lastName': fake.last_name(),
                'document': random.randint(10**11, 10**12),
                'documentType': random.choice(["CPF", "ID"]),
                'corporateName': "null",
                'tradeName': "null",
                'corporateDocument': "null",
                'isCorporate': random.choice(["true", "false"]),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'createdDate': fake.date_time_this_year().isoformat()
            },
            "shippingAddress": {
                "country": fake.country(),
                "street": fake.address(),
                "number": random.randint(1, 1000),
                "complement": "null",
                "neighborhood": fake.city(),
                "postalCode": fake.zipcode(),
                "city": fake.city(),
                "state": fake.state_abbr()
            },
            'billingAddress': {
                'country': fake.country(),
                'street': fake.address(),
                'number': random.randint(1, 1000),
                'complement': "null",
                'neighborhood': fake.city(),
                'postalCode': fake.zipcode(),
                'city': fake.city(),
                'state': fake.state_abbr()
            },
            'items': [{
                'id': random.randint(1, 999),
                'name': fake.company(),
                'price': round(random.uniform(10.0, 100.0), 2),
                'quantity':random.randint(1, 5),
                'discount': 0,
                'deliveryType': "Normal",
                'categoryId': "5",
                'sellerId': "1"
                }
            ],
            'shippingValue': 1,
            'taxValue': 0
        },
        'url': "https://admin.mystore.example.com/orders?q=1072430428324",
        'callbackUrl': "https://api.mystore.example.com/some-path/to-notify/status-changes?an=mystore",
        'returnUrl': "https://mystore.example.com/checkout/order/1072430428324",
        'inboundRequestsUrl': "https://api.mystore.example.com/checkout/order/1072430428324/inbound-request/:action",
        'recipients': [
            {
            'id': fake.company(),
            'name': fake.company(),
            'documentType': random.choice(["CNPJ", "CPF"]),
            'document': random.randint(10**11, 10**12),
            'role': "marketplace",
            'chargeProcessingFee': random.choice(["true", "false"]),
            'chargebackLiable': random.choice(["true", "false"]),
            'amount': round(random.uniform(10.0, 100.0), 2),
            }
            ],
        "merchantSettings": [{'name': "field1", 'value': "value1"}, {'name': "field2", 'value': "value2"}],
        'connectorMetadata': [{'name': "MetadataName", 'value': "MetadataValue" }]
        }

def send_fake_purchases():
    while True:
        for _ in range(3): # Number of JSONs on each batch, can be changed to speed up or slow down the generation speed.
            purchase_data = generate_purchase()
            print(f"Sending purchase data: {purchase_data}")
            producer.send(topic=topic, value=purchase_data)
        time.sleep(1)  # Wait for 1 second before generating the next batch, can be changed to speed up or slow down the generation speed.

if __name__ == "__main__":
    send_fake_purchases()