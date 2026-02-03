import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()
Faker.seed(42)
np.random.seed(42)

# -----------------------------
# CONFIG
# -----------------------------
N_USERS = 5000
N_ITEMS = 8000
N_SEARCHES = 20000
N_IMPRESSIONS = 60000
N_CLICKS = 15000
N_PURCHASES = 5000
N_NOTIFICATIONS = 12000
N_LIFECYCLE = 5000

# -----------------------------
# USERS
# -----------------------------
def generate_users(n=N_USERS):
    users = []
    for i in range(n):
        signup = fake.date_time_between(start_date="-2y", end_date="now")
        users.append({
            "user_id": i + 1,
            "signup_ts": signup,
            "country": fake.country_code(),
            "is_seller": np.random.choice([0, 1], p=[0.7, 0.3]),
            "marketing_opt_in": np.random.choice([0, 1], p=[0.6, 0.4])
        })
    return pd.DataFrame(users)

# -----------------------------
# ITEMS / LISTINGS
# -----------------------------
def generate_title(category):
    titles = {
        "Clothing": [
            "Vintage Denim Jacket",
            "Black Hoodie",
            "Floral Summer Dress",
            "Blue Skinny Jeans",
            "Wool Winter Coat",
            "Striped T-Shirt",
            "Corduroy Pants",
            "Oversized Sweater"
        ],
        "Shoes": [
            "Nike Running Shoes",
            "Leather Boots",
            "White Sneakers",
            "Heeled Sandals",
            "Trail Hiking Shoes",
            "Slip-On Canvas Shoes",
            "Platform Sneakers"
        ],
        "Accessories": [
            "Gold Hoop Earrings",
            "Leather Belt",
            "Canvas Tote Bag",
            "Silver Necklace",
            "Sunglasses",
            "Wool Scarf",
            "Minimalist Wallet"
        ],
        "Home": [
            "Wooden Coffee Table",
            "Ceramic Vase",
            "Floor Lamp",
            "Throw Blanket",
            "Wall Art Print",
            "Decorative Cushion",
            "Glass Storage Jar"
        ],
        "Electronics": [
            "Bluetooth Headphones",
            "Smartphone",
            "Portable Speaker",
            "Laptop Stand",
            "Wireless Mouse",
            "Noise-Cancelling Earbuds",
            "USB-C Charging Cable"
        ]
    }
    return random.choice(titles[category])

# -----------------------------------
# Generate items
# -----------------------------------
def generate_items(n=N_ITEMS):
    items = []
    categories = ["Clothing", "Shoes", "Accessories", "Home", "Electronics"]

    for i in range(n):
        category = random.choice(categories)
        created = fake.date_time_between(start_date="-18m", end_date="now")

        items.append({
            "item_id": i + 1,
            "seller_id": np.random.randint(1, 5000),  # random seller IDs
            "title": generate_title(category),
            "category": category,
            "price": round(np.random.uniform(5, 200), 2),
            "created_ts": created,
            "status": np.random.choice(["active", "sold", "removed"], p=[0.7, 0.25, 0.05])
        })

    return pd.DataFrame(items)

# -----------------------------
# SEARCH EVENTS
# -----------------------------
def generate_search_events(users, n=N_SEARCHES):
    events = []
    for i in range(n):
        user = random.choice(users["user_id"].tolist())
        ts = fake.date_time_between(start_date="-12m", end_date="now")
        events.append({
            "search_id": i + 1,
            "user_id": user,
            "query": random.choice(["dress", "shoes", "bag", "jacket", "lamp", "vintage", "nike", "adidas"]),
            "timestamp": ts
        })
    return pd.DataFrame(events)

# -----------------------------
# IMPRESSIONS
# -----------------------------
def generate_impressions(searches, items, n=N_IMPRESSIONS):
    events = []
    for i in range(n):
        search = searches.sample(1).iloc[0]
        item = items.sample(1).iloc[0]
        events.append({
            "impression_id": i + 1,
            "search_id": search["search_id"],
            "item_id": item["item_id"],
            "position": np.random.randint(1, 50),
            "timestamp": search["timestamp"] + timedelta(seconds=np.random.randint(1, 30))
        })
    return pd.DataFrame(events)

# -----------------------------
# CLICKS
# -----------------------------
def generate_clicks(impressions, n=N_CLICKS):
    clicks = impressions.sample(n)
    clicks = clicks.assign(
        click_ts=lambda df: df["timestamp"] + pd.to_timedelta(np.random.randint(1, 20, size=len(df)), unit="s")
    )
    clicks["click_id"] = range(1, len(clicks) + 1)
    return clicks[["click_id", "search_id", "item_id", "position", "click_ts"]]

# -----------------------------
# PURCHASES
# -----------------------------
def generate_purchases(clicks, items, n=N_PURCHASES):
    purchases = []
    sample_clicks = clicks.sample(n)
    for i, row in sample_clicks.iterrows():
        item = items[items["item_id"] == row["item_id"]].iloc[0]
        purchases.append({
            "purchase_id": len(purchases) + 1,
            "item_id": row["item_id"],
            "buyer_id": row["search_id"],  # simplified mapping
            "price": item["price"],
            "purchase_ts": row["click_ts"] + timedelta(minutes=np.random.randint(1, 60))
        })
    return pd.DataFrame(purchases)

# -----------------------------
# NOTIFICATIONS
# -----------------------------
def generate_notifications(users, n=N_NOTIFICATIONS):
    notifications = []
    for i in range(n):
        user = random.choice(users["user_id"].tolist())
        ts = fake.date_time_between(start_date="-12m", end_date="now")
        notifications.append({
            "notification_id": i + 1,
            "user_id": user,
            "type": random.choice(["promo", "reactivation", "price_drop", "new_message"]),
            "sent_ts": ts,
            "opened": np.random.choice([0, 1], p=[0.7, 0.3])
        })
    return pd.DataFrame(notifications)

# -----------------------------
# LIFECYCLE EVENTS
# -----------------------------
def generate_lifecycle(users, n=N_LIFECYCLE):
    events = []
    for i in range(n):
        user = random.choice(users["user_id"].tolist())
        ts = fake.date_time_between(start_date="-12m", end_date="now")
        events.append({
            "event_id": i + 1,
            "user_id": user,
            "event_type": random.choice(["activation", "churn", "reactivation"]),
            "timestamp": ts
        })
    return pd.DataFrame(events)

# -----------------------------
# RUN GENERATION
# -----------------------------
users = generate_users()
items = generate_items(users)
searches = generate_search_events(users)
impressions = generate_impressions(searches, items)
clicks = generate_clicks(impressions)
purchases = generate_purchases(clicks, items)
notifications = generate_notifications(users)
lifecycle = generate_lifecycle(users)

# -----------------------------
# SAVE TO CSV
# -----------------------------
users.to_csv("users.csv", index=False)
items.to_csv("items.csv", index=False)
searches.to_csv("search_events.csv", index=False)
impressions.to_csv("impressions.csv", index=False)
clicks.to_csv("clicks.csv", index=False)
purchases.to_csv("purchases.csv", index=False)
notifications.to_csv("notifications.csv", index=False)
lifecycle.to_csv("lifecycle_events.csv", index=False)

print("Synthetic marketplace data generated successfully!")
