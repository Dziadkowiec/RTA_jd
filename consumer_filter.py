from kafka import KafkaConsumer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

historia_uzytkownikow = {}

for message in consumer:
    print(message.value)
    if message.value['amount'] > 1000:
        print("ALERT")
    
    user_id = message.value['user_id']
    now = datetime.fromisoformat(message.value['timestamp'])

    if user_id not in historia_uzytkownikow:
        historia_uzytkownikow[user_id] = []

    historia_uzytkownikow[user_id].append(now)
    nowa_lista = []
    for i in historia_uzytkownikow[user_id]:
        roznica = (now - i).total_seconds()
        if roznica <= 60:
            nowa_lista.append(i)
    
    historia_uzytkownikow[user_id] = nowa_lista
    ilosc_transakcji = len(historia_uzytkownikow[user_id])

    if ilosc_transakcji > 3:
        print(f"ALERT ILOŚCI TRANSAKCJI DLA {user_id}")
