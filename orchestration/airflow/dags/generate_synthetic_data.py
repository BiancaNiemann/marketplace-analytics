import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import argparse
from pathlib import Path

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
# DATE RANGE HELPER
# -----------------------------
def get_current_month_range():
    """Get start and end dates for the current month"""
    now = datetime.now()
    start_of_month = datetime(now.year, now.month, 1)
    
    # Get first day of next month, then subtract 1 day
    if now.month == 12:
        end_of_month = datetime(now.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_of_month = datetime(now.year, now.month + 1, 1) - timedelta(days=1)
    
    return start_of_month, end_of_month

def clamp_to_month(timestamp, month_start, month_end):
    """Ensure timestamp stays within month boundaries"""
    if timestamp > month_end:
        return month_end
    return timestamp

# -----------------------------
# USERS
# -----------------------------
def generate_users(n=N_USERS, date_range=None):
    """Generate users who signed up in the current month"""
    users = []
    start_date, end_date = date_range if date_range else get_current_month_range()
    
    for i in range(n):
        signup = fake.date_time_between(start_date=start_date, end_date=end_date)
        users.append({
            "user_id": i + 1,
            "signup_ts": signup,
            
            # Demographics
            "country": fake.country_code(),
            "city": fake.city(),
            "age_group": np.random.choice(["18-24", "25-34", "35-44", "45-54", "55+"], 
                                         p=[0.25, 0.35, 0.20, 0.12, 0.08]),
            
            # User type & behavior
            "is_seller": np.random.choice([0, 1], p=[0.6, 0.4]),
            "account_type": np.random.choice(["basic", "premium"], p=[0.85, 0.15]),
            
            # Acquisition & engagement
            "signup_channel": np.random.choice(
                ["organic_search", "social_media", "email", "referral", "paid_ads", "direct"],
                p=[0.30, 0.25, 0.15, 0.15, 0.10, 0.05]
            ),
            "device_type": np.random.choice(["mobile", "desktop", "tablet"], 
                                           p=[0.65, 0.30, 0.05]),
            
            # Communication preferences
            "marketing_opt_in": np.random.choice([0, 1], p=[0.6, 0.4]),
            
            # Status
            "is_verified": np.random.choice([0, 1], p=[0.4, 0.6]),
            "status": np.random.choice(["active", "inactive", "suspended"], 
                                      p=[0.85, 0.13, 0.02])
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
def generate_items(n=N_ITEMS, date_range=None):
    items = []
    categories = ["Clothing", "Shoes", "Accessories", "Home", "Electronics"]
    start_date, end_date = date_range if date_range else get_current_month_range()

    for i in range(n):
        category = random.choice(categories)
        created = fake.date_time_between(start_date=start_date, end_date=end_date)

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
def generate_search_events(users, n=N_SEARCHES, date_range=None):
    events = []
    start_date, end_date = date_range if date_range else get_current_month_range()

    for i in range(n):
        user = random.choice(users["user_id"].tolist())
        ts = fake.date_time_between(start_date=start_date, end_date=end_date)
        events.append({
            "search_id": i + 1,
            "user_id": user,
            "query": random.choice([
                    # Clothing
                    "dress", "jeans", "jacket", "hoodie", "sweater", "coat", "shirt", "pants",
                    "skirt", "blazer", "cardigan", "shorts", "leggings", "jumpsuit",
                    
                    # Shoes
                    "shoes", "sneakers", "boots", "sandals", "heels", "flats", "loafers",
                    "running shoes", "hiking boots", "slippers",
                    
                    # Accessories
                    "bag", "backpack", "wallet", "belt", "scarf", "sunglasses", "watch",
                    "earrings", "necklace", "bracelet", "hat", "gloves", "tote bag",
                    
                    # Electronics
                    "headphones", "phone case", "laptop", "charger", "speaker", "mouse",
                    "keyboard", "tablet", "camera",
                    
                    # Brands (common searches)
                    "nike", "adidas", "zara", "levi's", "vintage", "handmade"
            ]),
            "timestamp": ts
        })
    return pd.DataFrame(events)

# -----------------------------
# IMPRESSIONS
# -----------------------------
def generate_impressions(searches, items, n=N_IMPRESSIONS, date_range=None):
    """Generate impressions based on searches"""
    events = []
    _, month_end = date_range if date_range else get_current_month_range()
    
    for i in range(n):
        search = searches.sample(1).iloc[0]
        item = items.sample(1).iloc[0]
        
        impression_ts = search["timestamp"] + timedelta(seconds=np.random.randint(1, 30))
        impression_ts = clamp_to_month(impression_ts, search["timestamp"], month_end)
        
        events.append({
            "impression_id": i + 1,
            "search_id": search["search_id"],
            "item_id": item["item_id"],
            "position": np.random.randint(1, 50),
            "timestamp": impression_ts
        })
    return pd.DataFrame(events)

# -----------------------------
# CLICKS
# -----------------------------
def generate_clicks(impressions, n=N_CLICKS, date_range=None):
    """Generate clicks from impressions"""
    _, month_end = date_range if date_range else get_current_month_range()
    
    clicks = impressions.sample(n).copy()
    
    # Add click timestamp with boundary check
    click_times = []
    for idx, row in clicks.iterrows():
        click_ts = row["timestamp"] + timedelta(seconds=np.random.randint(1, 20))
        click_ts = clamp_to_month(click_ts, row["timestamp"], month_end)
        click_times.append(click_ts)
    
    clicks["click_ts"] = click_times
    clicks["click_id"] = range(1, len(clicks) + 1)
    return clicks[["click_id", "search_id", "item_id", "position", "click_ts"]]

# -----------------------------
# PURCHASES
# -----------------------------
def generate_purchases(clicks, items, n=N_PURCHASES, date_range=None):
    """Generate purchases from clicks"""
    purchases = []
    _, month_end = date_range if date_range else get_current_month_range()
    
    sample_clicks = clicks.sample(n)
    for i, row in sample_clicks.iterrows():
        item = items[items["item_id"] == row["item_id"]].iloc[0]
        
        purchase_ts = row["click_ts"] + timedelta(minutes=np.random.randint(1, 60))
        purchase_ts = clamp_to_month(purchase_ts, row["click_ts"], month_end)
        
        purchases.append({
            "purchase_id": len(purchases) + 1,
            "item_id": row["item_id"],
            "buyer_id": row["search_id"],
            "price": item["price"],
            "purchase_ts": purchase_ts
        })
    return pd.DataFrame(purchases)

# -----------------------------
# NOTIFICATIONS
# -----------------------------
def generate_notifications(users, n=N_NOTIFICATIONS, date_range=None):
    notifications = []
    start_date, end_date = date_range if date_range else get_current_month_range()

    for i in range(n):
        user = random.choice(users["user_id"].tolist())
        ts = fake.date_time_between(start_date=start_date, end_date=end_date)
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
def generate_lifecycle(users, n=N_LIFECYCLE, date_range=None):
    events = []
    start_date, end_date = date_range if date_range else get_current_month_range()

    for i in range(n):
        user = random.choice(users["user_id"].tolist())
        ts = fake.date_time_between(start_date=start_date, end_date=end_date)
        events.append({
            "event_id": i + 1,
            "user_id": user,
            "event_type": random.choice(["activation", "churn", "reactivation"]),
            "timestamp": ts
        })
    return pd.DataFrame(events)
# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main(output_dir=None):
    """Generate synthetic data for current month"""
    
    # Get current month for folder naming
    now = datetime.now()
    month_folder = now.strftime("%Y-%m")
    
    # Set output directory
    if output_dir is None:
        output_dir = Path(f"csv_files/{month_folder}")
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating data for {month_folder}...")
    print(f"Output directory: {output_dir}")
    
    # Get date range for current month
    date_range = get_current_month_range()
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Generate all data
    users = generate_users(date_range=date_range)
    items = generate_items(date_range=date_range)
    searches = generate_search_events(users, date_range=date_range)
    impressions = generate_impressions(searches, items, date_range=date_range)  # Added date_range
    clicks = generate_clicks(impressions, date_range=date_range)  # Added date_range
    purchases = generate_purchases(clicks, items, date_range=date_range)  # Added date_range
    notifications = generate_notifications(users, date_range=date_range)
    lifecycle = generate_lifecycle(users, date_range=date_range)
    
    # Save to CSV
    users.to_csv(output_dir / "users.csv", index=False)
    items.to_csv(output_dir / "items.csv", index=False)
    searches.to_csv(output_dir / "search_events.csv", index=False)
    impressions.to_csv(output_dir / "impressions.csv", index=False)
    clicks.to_csv(output_dir / "clicks.csv", index=False)
    purchases.to_csv(output_dir / "purchases.csv", index=False)
    notifications.to_csv(output_dir / "notifications.csv", index=False)
    lifecycle.to_csv(output_dir / "lifecycle_events.csv", index=False)
    
    print(f"\n✓ Synthetic marketplace data generated successfully!")
    print(f"✓ Files saved to: {output_dir}")
    return str(output_dir)

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate monthly marketplace data')
    parser.add_argument('--output_dir', type=str, help='Output directory for CSV files')
    args = parser.parse_args()
    
    main(output_dir=args.output_dir)