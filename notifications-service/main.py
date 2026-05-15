from fastapi import FastAPI
from typing import List
from models import Notification, ErrorNotification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Consumer for successfully confirmed orders
    consumer_confirmed = AIOKafkaConsumer(
        "order-confirmed",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )

    # Consumer for product-not-found errors
    consumer_not_found = AIOKafkaConsumer(
        "product_not_found_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-not-found-group",
        auto_offset_reset="earliest"
    )

    # Consumer for out-of-stock errors
    consumer_out_of_stock = AIOKafkaConsumer(
        "out_of_stock_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-out-of-stock-group",
        auto_offset_reset="earliest"
    )

    await consumer_confirmed.start()
    await consumer_not_found.start()
    await consumer_out_of_stock.start()

    task_confirmed   = asyncio.create_task(consume_confirmed(consumer_confirmed))
    task_not_found   = asyncio.create_task(consume_error(consumer_not_found))
    task_out_of_stock = asyncio.create_task(consume_error(consumer_out_of_stock))

    yield

    for task in (task_confirmed, task_not_found, task_out_of_stock):
        task.cancel()
    await consumer_confirmed.stop()
    await consumer_not_found.stop()
    await consumer_out_of_stock.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []
error_notifications_db: List[ErrorNotification] = []

async def consume_confirmed(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Narudžbina {data['order_id']} za proizvod {data['product_id']} je uspešno potvrđena."
            )
            notifications_db.append(notification)
    except asyncio.CancelledError:
        pass

async def consume_error(consumer: AIOKafkaConsumer):
    """Generic error consumer — handles both product_not_found_events and out_of_stock_events."""
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            error_notification = ErrorNotification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                timestamp=data['timestamp'],
                error_reason=data['error_reason'],
                message=(
                    f"Narudžbina {data['order_id']} je odbijena. "
                    f"Razlog: {data['error_reason']}."
                )
            )
            error_notifications_db.append(error_notification)
    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db

@app.get("/notifications/errors", response_model=List[ErrorNotification])
def get_error_notifications():
    return error_notifications_db
