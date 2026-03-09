import random
import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
import uuid

fake = Faker()

# 1. Setup Connection
conn = mysql.connector.connect(
    host="localhost",
    port=3307,      
    user="root",
    password="123456",
    database="ecommerce_db",
    autocommit=False
)
cur = conn.cursor(dictionary=True)

# Initialize Global Trackers
existing_users = []
completed_purchase_map = {} # Tracks which users bought which products for reviews

def random_date_2026():
    start_date = datetime(2026, 1, 1)
    end_date = datetime.now()
    time_between = end_date - start_date
    seconds_between = time_between.total_seconds()
    random_seconds = random.randrange(int(seconds_between))
    return start_date + timedelta(seconds=random_seconds)

# Load initial promos (assuming table exists)
def insert_categories(max_categories=1000):
    data = [
        (
            fake.word().title() + " " + fake.word().title(),
            fake.sentence()
        )
        for _ in range(max_categories)
    ]
    cur.executemany(
        """
        INSERT INTO categories (category_name, description)
        VALUES (%s, %s)
        """,
        data
    )

    conn.commit()
    print(f"Inserted {len(data)} categories.")

def seed_products(n=100):
    cur.execute("SELECT category_id FROM categories")
    categories = [c["category_id"] for c in cur.fetchall()]
    
    for _ in range(n):
        cur.execute("""
            INSERT INTO products (category_id, product_name, base_price, stock_quantity)
            VALUES (%s, %s, %s, %s)
        """, (
            random.choice(categories),
            fake.word().title() + " " + fake.word().title(),
            random.randint(50, 2000),
            random.randint(500, 5000)
        ))
    conn.commit()
    print(f"Seeded {n} products")

def load_products():
    cur.execute("SELECT product_id, base_price, stock_quantity FROM products WHERE stock_quantity > 0")
    return cur.fetchall()

def get_or_create_user():
    if existing_users and random.random() < 0.7:
        return random.choice(existing_users)

    email = f"user_{uuid.uuid4().hex[:12]}@example.com"
    reg_date = random_date_2026() # Ngày tạo user
    cur.execute("""
        INSERT INTO users (full_name, email, country, registration_date) 
        VALUES (%s, %s, %s, %s)
    """, (fake.name(), email, fake.country(), reg_date))
    
    user_id = cur.lastrowid
    existing_users.append(user_id)
    return user_id

def pick_products(products_list):
    items = []
    total = 0
    # Pick 1-5 random products instead of shuffling the whole list
    selected = random.sample(products_list, k=min(len(products_list), random.randint(1, 5)))

    for p in selected:
        qty = random.randint(1, 3)
        # atomic update to prevent overselling
        cur.execute("""
            UPDATE products 
            SET stock_quantity = stock_quantity - %s 
            WHERE product_id = %s AND stock_quantity >= %s
        """, (qty, p["product_id"], qty))

        if cur.rowcount > 0:
            items.append((p["product_id"], qty, p["base_price"]))
            total += qty * p["base_price"]
    
    return items, total

def insert_order(products_list):
    user_id = get_or_create_user()
    order_date = random_date_2026()

    # 1. Xác định trạng thái và mã giảm giá
    status = random.choices(
        ["pending", "cancelled", "shipping", "completed"],
        [10, 5, 25, 60]
    )[0]

    # 2. Chọn ngẫu nhiên sản phẩm (Order Items)
    # Hàm pick_products trả về list các tuple: (product_id, qty, price)
    items, subtotal = pick_products(products_list)
    
    if not items: 
        return # Bỏ qua nếu không chọn được sản phẩm nào (hết hàng)

    # 3. Tính tổng tiền sau giảm giá
    final_total = subtotal

    # 4. Chèn vào bảng ORDERS
    cur.execute("""
        INSERT INTO orders (user_id, status, total_amount, created_at)
        VALUES (%s, %s, %s, %s)
    """, (user_id, status, round(final_total, 2), order_date))
    
    order_id = cur.lastrowid

    # 5. CHÈN VÀO BẢNG ORDER_ITEMS (Chi tiết đơn hàng)
    for product_id, qty, price in items:
        cur.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
        """, (order_id, product_id, qty, price))

    # 6. Xử lý thanh toán và lưu vết để đánh giá (nếu đã hoàn thành)
    if status == "completed":
        pay_date = order_date + timedelta(minutes=random.randint(5, 60))
        cur.execute("""
            INSERT INTO payments (order_id, payment_method, amount, payment_date) 
            VALUES (%s, %s, %s, %s)
        """, (order_id, random.choice(["credit_card", "paypal", "cod"]), round(final_total, 2), pay_date))
        
        for p_id, _, _ in items:
            completed_purchase_map.setdefault(p_id, set()).add(user_id)

    # 7. Xử lý vận chuyển
    if status in ("shipping", "completed"):
        # Ngày giao hàng phải sau ngày đặt hàng
        ship_date = order_date + timedelta(days=random.randint(1, 3))
        cur.execute("""
            INSERT INTO shipping_info (order_id, shipping_address, shipping_service, shipped_at)
            VALUES (%s, %s, %s, %s)
        """, (order_id, fake.address(), random.choice(["DHL", "FedEx", "UPS"]), ship_date))

# --- Execution ---
insert_categories(1000)
seed_products(3000) 
products_pool = load_products()

TOTAL_ORDERS = 10_000
BATCH_SIZE = 1000

print(f"Starting to insert {TOTAL_ORDERS} orders...")
for batch in range(TOTAL_ORDERS // BATCH_SIZE):
    try:
        for _ in range(BATCH_SIZE):
            insert_order(products_pool)
        conn.commit()
        print(f"Committed batch {batch + 1}/{(TOTAL_ORDERS // BATCH_SIZE)}")
    except Exception as e:
        conn.rollback()
        print(f"Error in batch {batch}: {e}")

# Insert Reviews based on completed purchases
def insert_reviews(max_reviews=1000):
    count = 0
    for product_id, users in completed_purchase_map.items():
        for user_id in list(users)[:3]:
            # Ngày review ngẫu nhiên từ lúc mua đến hiện tại
            review_date = random_date_2026() 
            cur.execute("""
                INSERT INTO product_reviews (product_id, user_id, rating, comment, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (product_id, user_id, random.randint(3, 5), fake.sentence(), review_date))
            count += 1
            if count >= max_reviews: break
        if count >= max_reviews: break
    conn.commit()
    print(f"Inserted {count} reviews.")

insert_reviews()