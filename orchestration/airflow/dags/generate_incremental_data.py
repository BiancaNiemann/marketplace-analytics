#!/usr/bin/env python3
"""
Incremental Marketplace Data Generator
Generates monthly data with:
- New users (500-600 per month)
- Activity for ALL users (both new and existing)
- Proper user state tracking
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import json
from google.cloud import storage
import os

fake = Faker()
Faker.seed(None)  # Use current time for randomness
np.random.seed(None)

# -----------------------------
# CONFIG
# -----------------------------
class Config:
    # Growth parameters
    NEW_USERS_MIN = 500
    NEW_USERS_MAX = 800
    
    # Activity scaling (per 1000 users)
    ITEMS_PER_1K_USERS = 1600
    SEARCHES_PER_1K_USERS = 4000
    IMPRESSIONS_PER_1K_USERS = 12000
    CLICKS_PER_1K_USERS = 3000
    PURCHASES_PER_1K_USERS = 1000
    NOTIFICATIONS_PER_1K_USERS = 2400
    LIFECYCLE_PER_1K_USERS = 1000
    
    # GCS Configuration
    PROJECT_ID = "marketplace-analytics-485915"
    BUCKET_NAME = f"{PROJECT_ID}-data-lake"
    USER_STATE_FILE = "state/user_state.json"

# -----------------------------
# USER STATE MANAGEMENT
# -----------------------------
class UserStateManager:
    """Manages user state across months using GCS"""
    
    def __init__(self, bucket_name, state_file_path):
        self.bucket_name = bucket_name
        self.state_file_path = state_file_path
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        
    def load_existing_users(self):
        """Load existing user state from GCS"""
        try:
            blob = self.bucket.blob(self.state_file_path)
            if blob.exists():
                state_json = blob.download_as_string()
                state = json.loads(state_json)
                print(f"✓ Loaded {len(state['users'])} existing users from state file")
                return pd.DataFrame(state['users']), state.get('last_user_id', 0)
            else:
                print("No existing user state found. Starting fresh.")
                return pd.DataFrame(), 0
        except Exception as e:
            print(f"Warning: Could not load user state: {e}")
            return pd.DataFrame(), 0
    
    def save_user_state(self, users_df, last_user_id):
        """Save updated user state to GCS"""
        try:
            state = {
                'users': users_df.to_dict('records'),
                'last_user_id': last_user_id,
                'updated_at': datetime.now().isoformat()
            }
            
            blob = self.bucket.blob(self.state_file_path)
            blob.upload_from_string(
                json.dumps(state, default=str),
                content_type='application/json'
            )
            print(f"✓ Saved user state with {len(users_df)} total users")
        except Exception as e:
            print(f"Error saving user state: {e}")
            raise

# -----------------------------
# DATE RANGE HELPERS
# -----------------------------
def get_month_range(year_month=None):
    """Get start and end dates for specified month (YYYY-MM format)"""
    if year_month:
        year, month = map(int, year_month.split('-'))
    else:
        now = datetime.now()
        year, month = now.year, now.month
    
    start_of_month = datetime(year, month, 1)
    
    if month == 12:
        end_of_month = datetime(year + 1, 1, 1) - timedelta(seconds=1)
    else:
        end_of_month = datetime(year, month + 1, 1) - timedelta(seconds=1)
    
    return start_of_month, end_of_month

def random_timestamp_in_range(start_date, end_date):
    """Generate random timestamp within date range"""
    time_between = end_date - start_date
    random_seconds = np.random.randint(0, int(time_between.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)

def clamp_to_month(timestamp, month_start, month_end):
    """Ensure timestamp stays within month boundaries"""
    if timestamp < month_start:
        return month_start
    if timestamp > month_end:
        return month_end
    return timestamp

# -----------------------------
# USER GENERATION
# -----------------------------
def generate_new_users(n_new_users, starting_user_id, date_range):
    """Generate new users for current month"""
    users = []
    start_date, end_date = date_range
    
    for i in range(n_new_users):
        user_id = starting_user_id + i + 1
        signup_ts = random_timestamp_in_range(start_date, end_date)
        
        users.append({
            "user_id": user_id,
            "email": fake.email(),
            "signup_date": signup_ts.date(),
            "signup_ts": signup_ts,
            
            # Demographics
            "country": np.random.choice(['US', 'GB', 'DE', 'FR', 'NL', 'BE', 'IT', 'ES'], 
                                       p=[0.30, 0.20, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05]),
            "city": fake.city(),
            "age_group": np.random.choice(["18-24", "25-34", "35-44", "45-54", "55+"], 
                                         p=[0.25, 0.35, 0.20, 0.12, 0.08]),
            
            # User characteristics
            "is_seller": np.random.choice([0, 1], p=[0.6, 0.4]),
            "account_type": np.random.choice(["basic", "premium"], p=[0.85, 0.15]),
            
            # Acquisition
            "signup_channel": np.random.choice(
                ["organic_search", "social_media", "email", "referral", "paid_ads", "direct"],
                p=[0.30, 0.25, 0.15, 0.15, 0.10, 0.05]
            ),
            "device_type": np.random.choice(["mobile", "desktop", "tablet"], 
                                           p=[0.65, 0.30, 0.05]),
            
            # Preferences
            "marketing_opt_in": np.random.choice([0, 1], p=[0.6, 0.4]),
            "is_verified": np.random.choice([0, 1], p=[0.4, 0.6]),
            "status": "active"
        })
    
    return pd.DataFrame(users)

def update_existing_user_activity(users_df, date_range):
    """Update activity status for existing users based on their behavior"""
    # Simulate some users becoming inactive or reactivating
    for idx in users_df.index:
        current_status = users_df.at[idx, 'status']
        
        if current_status == 'active':
            # 5% chance of becoming inactive
            if np.random.random() < 0.05:
                users_df.at[idx, 'status'] = 'inactive'
        elif current_status == 'inactive':
            # 10% chance of reactivating
            if np.random.random() < 0.10:
                users_df.at[idx, 'status'] = 'active'
        
        # Very small chance of suspension
        if np.random.random() < 0.001:
            users_df.at[idx, 'status'] = 'suspended'
    
    return users_df

# -----------------------------
# ITEMS / LISTINGS
# -----------------------------
def generate_title(category):
    """Generate realistic product titles"""
    titles = {
        "Clothing": [
            "Vintage Denim Jacket", "Black Hoodie", "Floral Summer Dress",
            "Blue Skinny Jeans", "Wool Winter Coat", "Striped T-Shirt",
            "Corduroy Pants", "Oversized Sweater", "Leather Jacket",
            "Midi Skirt", "Linen Shirt", "Cotton Cardigan"
        ],
        "Shoes": [
            "Nike Running Shoes", "Leather Boots", "White Sneakers",
            "Heeled Sandals", "Trail Hiking Shoes", "Slip-On Canvas Shoes",
            "Platform Sneakers", "Ankle Boots", "Loafers"
        ],
        "Accessories": [
            "Gold Hoop Earrings", "Leather Belt", "Canvas Tote Bag",
            "Silver Necklace", "Sunglasses", "Wool Scarf",
            "Minimalist Wallet", "Crossbody Bag", "Watch"
        ],
        "Home": [
            "Wooden Coffee Table", "Ceramic Vase", "Floor Lamp",
            "Throw Blanket", "Wall Art Print", "Decorative Cushion",
            "Glass Storage Jar", "Bookshelf", "Table Lamp"
        ],
        "Electronics": [
            "Bluetooth Headphones", "Smartphone Case", "Portable Speaker",
            "Laptop Stand", "Wireless Mouse", "Noise-Cancelling Earbuds",
            "USB-C Cable", "Power Bank", "Tablet"
        ]
    }
    return np.random.choice(titles[category])

def generate_items(users_df, n_items, date_range):
    """Generate items from all users (weighted by seller status)"""
    items = []
    start_date, end_date = date_range
    
    # Get potential sellers (weighted by is_seller flag)
    sellers = users_df[users_df['is_seller'] == 1]['user_id'].tolist()
    all_users = users_df['user_id'].tolist()
    
    # Mix of dedicated sellers and occasional sellers
    seller_pool = sellers * 3 + all_users  # 75% from sellers, 25% from all users
    
    categories = ["Clothing", "Shoes", "Accessories", "Home", "Electronics"]
    
    for i in range(n_items):
        seller_id = np.random.choice(seller_pool)
        category = np.random.choice(categories, p=[0.40, 0.20, 0.15, 0.15, 0.10])
        created_ts = random_timestamp_in_range(start_date, end_date)
        
        # Price varies by category
        if category == "Electronics":
            price = round(np.random.uniform(20, 500), 2)
        elif category == "Home":
            price = round(np.random.uniform(15, 300), 2)
        else:
            price = round(np.random.uniform(5, 150), 2)
        
        items.append({
            "item_id": i + 1,
            "seller_id": seller_id,
            "title": generate_title(category),
            "category": category,
            "price": price,
            "currency": "USD",
            "condition": np.random.choice(["new", "like_new", "good", "fair"], 
                                         p=[0.10, 0.30, 0.45, 0.15]),
            "listed_date": created_ts.date(),
            "created_ts": created_ts,
            "status": np.random.choice(["active", "sold", "removed"], 
                                      p=[0.70, 0.25, 0.05])
        })
    
    return pd.DataFrame(items)

# -----------------------------
# SEARCH EVENTS
# -----------------------------
def generate_search_events(users_df, n_searches, date_range):
    """Generate searches from active users"""
    searches = []
    start_date, end_date = date_range
    
    # Get active users (more likely to search)
    active_users = users_df[users_df['status'] == 'active']['user_id'].tolist()
    all_users = users_df['user_id'].tolist()
    
    # 80% from active users, 20% from all users
    user_pool = active_users * 4 + all_users
    
    search_queries = [
        # Clothing
        "dress", "jeans", "jacket", "hoodie", "sweater", "coat", "shirt", "pants",
        "vintage denim", "black dress", "winter coat", "summer dress",
        
        # Shoes
        "shoes", "sneakers", "boots", "sandals", "heels", "running shoes",
        "white sneakers", "leather boots",
        
        # Accessories
        "bag", "backpack", "wallet", "belt", "scarf", "sunglasses", "jewelry",
        "gold earrings", "leather bag",
        
        # Electronics
        "headphones", "phone case", "laptop", "speaker", "mouse",
        
        # Brands
        "nike", "adidas", "zara", "levi's", "vintage", "secondhand"
    ]
    
    for i in range(n_searches):
        user_id = np.random.choice(user_pool)
        search_ts = random_timestamp_in_range(start_date, end_date)
        
        searches.append({
            "search_id": i + 1,
            "user_id": user_id,
            "query": np.random.choice(search_queries),
            "timestamp": search_ts,
            "search_date": search_ts.date()
        })
    
    return pd.DataFrame(searches)

# -----------------------------
# IMPRESSIONS
# -----------------------------
def generate_impressions(searches_df, items_df, n_impressions, date_range):
    """Generate impressions from searches"""
    impressions = []
    _, month_end = date_range
    
    for i in range(n_impressions):
        search = searches_df.sample(1).iloc[0]
        item = items_df.sample(1).iloc[0]
        
        # Impression happens seconds after search
        impression_ts = search["timestamp"] + timedelta(seconds=np.random.randint(0, 30))
        impression_ts = clamp_to_month(impression_ts, search["timestamp"], month_end)
        
        impressions.append({
            "impression_id": i + 1,
            "search_id": search["search_id"],
            "user_id": search["user_id"],
            "item_id": item["item_id"],
            "position": np.random.randint(1, 51),
            "timestamp": impression_ts,
            "impression_date": impression_ts.date()
        })
    
    return pd.DataFrame(impressions)

# -----------------------------
# CLICKS
# -----------------------------
def generate_clicks(impressions_df, n_clicks, date_range):
    """Generate clicks from impressions (higher positions = higher CTR)"""
    _, month_end = date_range
    
    # Weight by position (position 1 more likely to be clicked)
    impressions_df['click_weight'] = 1 / (impressions_df['position'] ** 0.5)
    clicks_sample = impressions_df.sample(
        n=n_clicks, 
        weights='click_weight',
        replace=False
    ).copy()
    
    click_data = []
    for idx, row in clicks_sample.iterrows():
        click_ts = row["timestamp"] + timedelta(seconds=np.random.randint(1, 120))
        click_ts = clamp_to_month(click_ts, row["timestamp"], month_end)
        
        click_data.append({
            "click_id": len(click_data) + 1,
            "impression_id": row["impression_id"],
            "search_id": row["search_id"],
            "user_id": row["user_id"],
            "item_id": row["item_id"],
            "position": row["position"],
            "click_ts": click_ts,
            "click_date": click_ts.date()
        })
    
    return pd.DataFrame(click_data)

# -----------------------------
# PURCHASES
# -----------------------------
def generate_purchases(clicks_df, items_df, n_purchases, date_range):
    """Generate purchases from clicks"""
    purchases = []
    _, month_end = date_range
    
    # Sample clicks for purchases
    purchase_clicks = clicks_df.sample(n=min(n_purchases, len(clicks_df)))
    
    for idx, click in purchase_clicks.iterrows():
        item = items_df[items_df['item_id'] == click['item_id']].iloc[0]
        
        # Purchase happens minutes to hours after click
        purchase_ts = click["click_ts"] + timedelta(
            minutes=np.random.randint(1, 480)  # 1 min to 8 hours
        )
        purchase_ts = clamp_to_month(purchase_ts, click["click_ts"], month_end)
        
        # Platform takes 10-15% fee
        platform_fee = round(item['price'] * np.random.uniform(0.10, 0.15), 2)
        
        purchases.append({
            "purchase_id": len(purchases) + 1,
            "item_id": click["item_id"],
            "buyer_id": click["user_id"],
            "seller_id": item["seller_id"],
            "purchase_amount": item["price"],
            "platform_fee": platform_fee,
            "payment_method": np.random.choice(
                ["credit_card", "paypal", "apple_pay", "google_pay"],
                p=[0.50, 0.30, 0.12, 0.08]
            ),
            "currency": "USD",
            "purchase_timestamp": purchase_ts,
            "purchase_date": purchase_ts.date()
        })
    
    return pd.DataFrame(purchases)

# -----------------------------
# NOTIFICATIONS
# -----------------------------
def generate_notifications(users_df, n_notifications, date_range):
    """Generate notifications for users"""
    notifications = []
    start_date, end_date = date_range
    
    # Focus on users who opted in to marketing
    opted_in = users_df[users_df['marketing_opt_in'] == 1]['user_id'].tolist()
    all_users = users_df['user_id'].tolist()
    
    # 70% to opted-in users, 30% to all (transactional)
    user_pool = opted_in * 7 + all_users * 3
    
    notification_types = [
        ("promo", 0.30, 0.25),  # (type, proportion, open_rate)
        ("price_drop", 0.25, 0.35),
        ("new_message", 0.20, 0.60),
        ("item_sold", 0.15, 0.80),
        ("reactivation", 0.10, 0.15)
    ]
    
    for i in range(n_notifications):
        user_id = np.random.choice(user_pool)
        notif_ts = random_timestamp_in_range(start_date, end_date)
        
        # Select notification type
        notif_type, _, open_rate = notification_types[
            np.random.choice(len(notification_types), 
                           p=[t[1] for t in notification_types])
        ]
        
        notifications.append({
            "notification_id": i + 1,
            "user_id": user_id,
            "type": notif_type,
            "sent_ts": notif_ts,
            "sent_date": notif_ts.date(),
            "opened": np.random.choice([0, 1], p=[1-open_rate, open_rate]),
            "channel": np.random.choice(["email", "push", "sms"], 
                                       p=[0.60, 0.35, 0.05])
        })
    
    return pd.DataFrame(notifications)

# -----------------------------
# LIFECYCLE EVENTS
# -----------------------------
def generate_lifecycle_events(users_df, n_events, date_range):
    """Generate lifecycle events"""
    events = []
    start_date, end_date = date_range
    
    all_users = users_df['user_id'].tolist()
    
    event_types = [
        ("activation", 0.40),
        ("purchase", 0.30),
        ("listing_created", 0.20),
        ("profile_completed", 0.10)
    ]
    
    for i in range(n_events):
        user_id = np.random.choice(all_users)
        event_ts = random_timestamp_in_range(start_date, end_date)
        
        event_type = np.random.choice(
            [e[0] for e in event_types],
            p=[e[1] for e in event_types]
        )
        
        events.append({
            "event_id": i + 1,
            "user_id": user_id,
            "event_type": event_type,
            "timestamp": event_ts,
            "event_date": event_ts.date(),
            "properties": "{}"  # Placeholder for JSON properties
        })
    
    return pd.DataFrame(events)

# -----------------------------
# MAIN GENERATION FUNCTION
# -----------------------------
def generate_monthly_data(year_month=None, output_dir=None, use_gcs_state=True):
    """
    Generate monthly marketplace data
    
    Args:
        year_month: Month in YYYY-MM format (defaults to current month)
        output_dir: Local output directory
        use_gcs_state: Whether to use GCS for state management
    """
    
    # Determine month
    if year_month is None:
        year_month = datetime.now().strftime('%Y-%m')
    
    print(f"\n{'='*60}")
    print(f"Generating Marketplace Data for {year_month}")
    print(f"{'='*60}\n")
    
    # Get date range
    date_range = get_month_range(year_month)
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Set up state management
    if use_gcs_state:
        state_mgr = UserStateManager(Config.BUCKET_NAME, Config.USER_STATE_FILE)
        existing_users_df, last_user_id = state_mgr.load_existing_users()
    else:
        existing_users_df = pd.DataFrame()
        last_user_id = 0
    
    last_user_id = int(last_user_id)
        
    # Generate new users
    n_new_users = np.random.randint(Config.NEW_USERS_MIN, Config.NEW_USERS_MAX + 1)
    print(f"\nGenerating {n_new_users} new users (starting from ID {last_user_id + 1})")
    new_users_df = generate_new_users(n_new_users, last_user_id, date_range)
    
    # Combine with existing users
    if len(existing_users_df) > 0:
        # Update existing user statuses
        existing_users_df = update_existing_user_activity(existing_users_df, date_range)
        all_users_df = pd.concat([existing_users_df, new_users_df], ignore_index=True)
    else:
        all_users_df = new_users_df
    
    total_users = len(all_users_df)
    print(f"Total user base: {total_users} users")
    
    # Calculate activity volumes based on total users
    scale = total_users / 1000
    n_items = int(Config.ITEMS_PER_1K_USERS * scale)
    n_searches = int(Config.SEARCHES_PER_1K_USERS * scale)
    n_impressions = int(Config.IMPRESSIONS_PER_1K_USERS * scale)
    n_clicks = int(Config.CLICKS_PER_1K_USERS * scale)
    n_purchases = int(Config.PURCHASES_PER_1K_USERS * scale)
    n_notifications = int(Config.NOTIFICATIONS_PER_1K_USERS * scale)
    n_lifecycle = int(Config.LIFECYCLE_PER_1K_USERS * scale)
    
    print(f"\nGenerating activity data (scaled for {total_users} users):")
    print(f"  - Items: {n_items}")
    print(f"  - Searches: {n_searches}")
    print(f"  - Impressions: {n_impressions}")
    print(f"  - Clicks: {n_clicks}")
    print(f"  - Purchases: {n_purchases}")
    print(f"  - Notifications: {n_notifications}")
    print(f"  - Lifecycle events: {n_lifecycle}")
    
    # Generate all activity data
    items_df = generate_items(all_users_df, n_items, date_range)
    searches_df = generate_search_events(all_users_df, n_searches, date_range)
    impressions_df = generate_impressions(searches_df, items_df, n_impressions, date_range)
    clicks_df = generate_clicks(impressions_df, n_clicks, date_range)
    purchases_df = generate_purchases(clicks_df, items_df, n_purchases, date_range)
    notifications_df = generate_notifications(all_users_df, n_notifications, date_range)
    lifecycle_df = generate_lifecycle_events(all_users_df, n_lifecycle, date_range)
    
    # Set output directory
    if output_dir is None:
        output_dir = Path(f"csv_files/{year_month}")
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save monthly data (only new users, but all activity)
    print(f"\nSaving data to {output_dir}")
    
    # Only save NEW users to monthly file (not all users)
    new_users_df.to_csv(output_dir / "users.csv", index=False)
    items_df.to_csv(output_dir / "items.csv", index=False)
    searches_df.to_csv(output_dir / "search_events.csv", index=False)
    impressions_df.to_csv(output_dir / "impressions.csv", index=False)
    clicks_df.to_csv(output_dir / "clicks.csv", index=False)
    purchases_df.to_csv(output_dir / "purchases.csv", index=False)
    notifications_df.to_csv(output_dir / "notifications.csv", index=False)
    lifecycle_df.to_csv(output_dir / "lifecycle_events.csv", index=False)
    
    print(f"\n✓ Data generation complete!")
    print(f"  - New users saved: {len(new_users_df)}")
    print(f"  - Activity generated for all {total_users} users")
    
    # Save updated user state to GCS
    if use_gcs_state:
        new_last_user_id = all_users_df['user_id'].max()
        state_mgr.save_user_state(all_users_df, new_last_user_id)
    
    return str(output_dir)

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate incremental monthly marketplace data'
    )
    parser.add_argument(
        '--year_month',
        type=str,
        help='Month in YYYY-MM format (default: current month)'
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        help='Output directory for CSV files'
    )
    parser.add_argument(
        '--no-gcs-state',
        action='store_true',
        help='Disable GCS state management (for testing)'
    )
    
    args = parser.parse_args()
    
    generate_monthly_data(
        year_month=args.year_month,
        output_dir=args.output_dir,
        use_gcs_state=not args.no_gcs_state
    )