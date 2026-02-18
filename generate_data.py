import pandas as pd
import random
import csv
from datetime import datetime, timedelta

random.seed(42)

# ── CUSTOMERS ────────────────────────────────────────────────────
first_names = ["Aarav","Priya","Rahul","Sneha","Vikram","Ananya","Rohan","Kavya",
                "Arjun","Meera","Siddharth","Pooja","Karan","Divya","Nikhil",
                "James","Emma","Liam","Olivia","Noah","Ava","William","Sophia"]
last_names  = ["Sharma","Patel","Singh","Kumar","Mehta","Iyer","Reddy","Nair",
                "Smith","Johnson","Williams","Brown","Jones","Garcia","Martinez"]
cities = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Pune","Kolkata",
          "London","New York","Dubai","Singapore","Sydney"]
segments = ["Premium","Standard","Basic"]

customers = []
for i in range(1, 501):
    customers.append({
        "customer_id": f"C{i:04d}",
        "first_name": random.choice(first_names),
        "last_name": random.choice(last_names),
        "email": f"customer{i}@email.com",
        "city": random.choice(cities),
        "segment": random.choice(segments),
        "registration_date": (datetime(2021, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime("%Y-%m-%d"),
        "is_active": random.choice([1, 1, 1, 0]),
        "record_start_date": "2021-01-01",
        "record_end_date": None,
        "is_current": 1
    })

pd.DataFrame(customers).to_csv("/home/claude/retail-lakehouse/data/customers.csv", index=False)
print(f"customers.csv → {len(customers)} rows")

# ── PRODUCTS ─────────────────────────────────────────────────────
categories = {
    "Electronics":    ["Laptop","Smartphone","Tablet","Headphones","Smartwatch","Camera","Speaker"],
    "Clothing":       ["T-Shirt","Jeans","Jacket","Dress","Sneakers","Kurta","Saree"],
    "Home & Kitchen": ["Mixer","Pressure Cooker","Bedsheet","Curtains","Air Purifier","Sofa"],
    "Books":          ["Fiction Novel","Data Engineering","Python Cookbook","Self Help","Biography"],
    "Sports":         ["Cricket Bat","Yoga Mat","Dumbbells","Running Shoes","Badminton Racket"],
}
brands = ["Samsung","Apple","Nike","Adidas","Sony","LG","Philips","Prestige","Puma","boAt"]

products = []
pid = 1
for cat, items in categories.items():
    for item in items:
        for _ in range(random.randint(2, 5)):
            price = round(random.uniform(199, 49999), 2)
            products.append({
                "product_id": f"P{pid:04d}",
                "product_name": item + " " + random.choice(["Pro","Lite","Plus","Max","Standard"]),
                "category": cat,
                "brand": random.choice(brands),
                "price": price,
                "cost_price": round(price * random.uniform(0.4, 0.7), 2),
                "stock_qty": random.randint(0, 500),
                "launched_date": (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 900))).strftime("%Y-%m-%d"),
            })
            pid += 1

pd.DataFrame(products).to_csv("/home/claude/retail-lakehouse/data/products.csv", index=False)
print(f"products.csv → {len(products)} rows")

# ── STORES ───────────────────────────────────────────────────────
store_cities = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Pune","Kolkata","Jaipur","Ahmedabad","Surat"]
stores = []
for i, city in enumerate(store_cities, 1):
    stores.append({
        "store_id": f"S{i:03d}",
        "store_name": f"{city} Retail Hub",
        "city": city,
        "region": "North" if city in ["Delhi","Jaipur"] else ("South" if city in ["Bengaluru","Hyderabad","Chennai"] else ("East" if city == "Kolkata" else "West")),
        "store_type": random.choice(["Flagship","Express","Online"]),
        "opened_date": (datetime(2018, 1, 1) + timedelta(days=random.randint(0, 1400))).strftime("%Y-%m-%d"),
    })

pd.DataFrame(stores).to_csv("/home/claude/retail-lakehouse/data/stores.csv", index=False)
print(f"stores.csv → {len(stores)} rows")

# ── TRANSACTIONS (500K+) ─────────────────────────────────────────
payment_methods = ["Credit Card","Debit Card","UPI","Net Banking","Cash","EMI","Wallet"]
statuses = ["Completed","Completed","Completed","Returned","Cancelled"]

rows = []
order_id = 1
for _ in range(500_000):
    order_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
    product    = random.choice(products)
    qty        = random.randint(1, 5)
    unit_price = product["price"]
    discount   = round(random.uniform(0, 0.3) * unit_price, 2)
    rows.append({
        "transaction_id": f"T{order_id:07d}",
        "customer_id":    random.choice(customers)["customer_id"],
        "product_id":     product["product_id"],
        "store_id":       random.choice(stores)["store_id"],
        "order_date":     order_date.strftime("%Y-%m-%d"),
        "quantity":       qty,
        "unit_price":     unit_price,
        "discount_amount":discount,
        "total_amount":   round((unit_price - discount) * qty, 2),
        "payment_method": random.choice(payment_methods),
        "status":         random.choice(statuses),
    })
    order_id += 1

pd.DataFrame(rows).to_csv("/home/claude/retail-lakehouse/data/transactions.csv", index=False)
print(f"transactions.csv → {len(rows)} rows")

print("\nAll sample data generated successfully!")
