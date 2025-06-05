CREATE TABLE fact_message (
    message_id TEXT PRIMARY KEY,
    room_id TEXT,
    room_created_at TIMESTAMP,
    channel TEXT,
    customer_id TEXT,
    customer_name TEXT,
    phone TEXT,
    sender_type TEXT,
    message_text TEXT,
    message_date TIMESTAMP
);
