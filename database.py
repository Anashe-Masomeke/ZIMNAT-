"""
database.py - SQLite database for Zimnat Rewards (v3 — Life Assurance focused)
"""

import sqlite3
import os

DB_FILE = "zimnat_rewards.db"


def get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone_number TEXT UNIQUE NOT NULL,
        full_name TEXT NOT NULL,
        national_id TEXT UNIQUE NOT NULL,
        pin TEXT NOT NULL,
        business_unit TEXT NOT NULL DEFAULT 'LIFE',
        total_points INTEGER NOT NULL DEFAULT 0,
        lifetime_points INTEGER NOT NULL DEFAULT 0,
        tier TEXT NOT NULL DEFAULT 'Bronze',
        referral_code TEXT UNIQUE,
        referred_by TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now')),
        last_activity TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL REFERENCES customers(id),
        account_type TEXT NOT NULL,
        account_number TEXT UNIQUE NOT NULL,
        policy_type TEXT,
        payment_start_date TEXT,
        monthly_payment_usd REAL NOT NULL DEFAULT 0.0,
        monthly_payment_zig REAL NOT NULL DEFAULT 0.0,
        currency TEXT NOT NULL DEFAULT 'USD',
        status TEXT NOT NULL DEFAULT 'Active',
        members_covered TEXT DEFAULT '[]',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS points_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL REFERENCES customers(id),
        activity TEXT NOT NULL,
        points INTEGER NOT NULL,
        description TEXT,
        reference TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS rewards_catalogue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        points_required INTEGER NOT NULL,
        reward_type TEXT NOT NULL,
        business_unit TEXT DEFAULT 'ALL',
        tier_required TEXT DEFAULT 'Bronze',
        stock INTEGER DEFAULT -1,
        available INTEGER NOT NULL DEFAULT 1
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS redemptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL REFERENCES customers(id),
        reward_id INTEGER NOT NULL REFERENCES rewards_catalogue(id),
        points_used INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'Approved',
        notes TEXT,
        redeemed_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS payment_reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL REFERENCES customers(id),
        account_number TEXT NOT NULL,
        amount_due REAL NOT NULL,
        due_date TEXT NOT NULL,
        reminder_sent INTEGER DEFAULT 0,
        status TEXT DEFAULT 'Pending',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL REFERENCES customers(id),
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        type TEXT DEFAULT 'INFO',
        is_read INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS cross_sell_offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL REFERENCES customers(id),
        product_name TEXT NOT NULL,
        business_unit TEXT NOT NULL,
        description TEXT,
        bonus_points INTEGER DEFAULT 0,
        status TEXT DEFAULT 'Active',
        viewed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS referral_tracking (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER NOT NULL REFERENCES customers(id),
        referee_phone TEXT NOT NULL,
        referee_id INTEGER REFERENCES customers(id),
        points_awarded INTEGER DEFAULT 0,
        status TEXT DEFAULT 'Pending',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS kpi_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT DEFAULT (date('now')),
        total_customers INTEGER DEFAULT 0,
        active_users INTEGER DEFAULT 0,
        points_issued INTEGER DEFAULT 0,
        points_redeemed INTEGER DEFAULT 0,
        on_time_payments INTEGER DEFAULT 0,
        total_payments INTEGER DEFAULT 0,
        new_referrals INTEGER DEFAULT 0,
        cross_sell_clicks INTEGER DEFAULT 0
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS admin_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT DEFAULT 'admin',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS ussd_sessions (
        session_id TEXT PRIMARY KEY,
        phone_number TEXT NOT NULL,
        menu_state TEXT NOT NULL DEFAULT 'MAIN',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""")

    # Seed rewards catalogue
    cur.execute("SELECT COUNT(*) FROM rewards_catalogue")
    if cur.fetchone()[0] == 0:
        rewards = [
            ("Airtime $1",              "Mobile airtime worth $1",                   100,  "DIGITAL",   "ALL",     "Bronze",  -1, 1),
            ("Airtime $3",              "Mobile airtime worth $3",                   270,  "DIGITAL",   "ALL",     "Bronze",  -1, 1),
            ("Airtime $5",              "Mobile airtime worth $5",                   450,  "DIGITAL",   "ALL",     "Bronze",  -1, 1),
            ("Mobile Data 500MB",       "500MB mobile data bundle",                  200,  "DIGITAL",   "ALL",     "Bronze",  -1, 1),
            ("Mobile Data 1GB",         "1GB mobile data bundle",                    350,  "DIGITAL",   "ALL",     "Silver",  -1, 1),
            ("Mobile Data 5GB",         "5GB mobile data bundle",                    900,  "DIGITAL",   "ALL",     "Gold",    -1, 1),
            ("Zimnat Notebook",         "Branded A5 notebook",                       300,  "PHYSICAL",  "ALL",     "Bronze",  50, 1),
            ("Zimnat Lunch Bag",        "Zimnat branded lunch bag",                  600,  "PHYSICAL",  "ALL",     "Silver",  30, 1),
            ("Zimnat Umbrella",         "Branded Zimnat umbrella",                   800,  "PHYSICAL",  "ALL",     "Silver",  20, 1),
            ("Zimnat Regalia Pack",     "Cap, T-shirt and pen set",                 1200,  "PHYSICAL",  "ALL",     "Gold",    10, 1),
            ("Premium Discount 10%",    "10% off next premium payment",              500,  "FINANCIAL", "LIFE",    "Bronze",  -1, 1),
            ("Bonus Cover 5%",          "5% bonus cover increase on your policy",    800,  "FINANCIAL", "LIFE",    "Silver",  -1, 1),
            ("Extra Member Free",       "Add one extra member at no cost for a year",1200, "FINANCIAL", "LIFE",    "Gold",    -1, 1),
            ("Claims Priority",         "Priority claims processing",                400,  "FINANCIAL", "LIFE",    "Bronze",  -1, 1),
            ("Roadside Assist Free",    "Free roadside assistance add-on",           600,  "FINANCIAL", "GENERAL", "Bronze",  -1, 1),
            ("Grace Period 1 Month",    "1-month loan grace period",                 700,  "FINANCIAL", "ZFS",     "Bronze",  -1, 1),
            ("Interest Rate Cut",       "0.5% interest reduction on next loan",      900,  "FINANCIAL", "ZFS",     "Silver",  -1, 1),
            ("Bonus Invest Units",      "Bonus investment units added",              700,  "FINANCIAL", "ASSET",   "Silver",  -1, 1),
            ("Advisory Session",        "Free investment advisory session",          500,  "FINANCIAL", "ASSET",   "Bronze",  -1, 1),
        ]
        cur.executemany("""INSERT INTO rewards_catalogue
            (name, description, points_required, reward_type, business_unit, tier_required, stock, available)
            VALUES (?,?,?,?,?,?,?,?)""", rewards)

    # Seed admin
    import hashlib
    cur.execute("SELECT COUNT(*) FROM admin_users")
    if cur.fetchone()[0] == 0:
        pw = hashlib.sha256("admin123".encode()).hexdigest()
        cur.execute("INSERT INTO admin_users (username, password, role) VALUES (?,?,?)",
                    ("admin", pw, "superadmin"))

    conn.commit()
    conn.close()
    print("[DB] Database initialised (v3 — Life Assurance).")


if __name__ == "__main__":
    init_db()
