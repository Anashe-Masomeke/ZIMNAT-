"""
models.py - Business logic for Zimnat Rewards (v3 — Life Assurance focused)
Login by National ID. Policy-based accounts. Proper points enforcement.
"""

import hashlib
import random
import string
import json
from datetime import date, datetime
from database import get_connection

POINTS_TABLE = {
    "ON_TIME_PAYMENT":    50,
    "EARLY_PAYMENT":      80,
    "POLICY_RENEWAL":     70,
    "REFERRAL":          100,
    "USSD_CHECK":         20,
    "UPDATE_DETAILS":     30,
    "REGULAR_INVEST":     60,
    "LONG_TERM_HOLD":     90,
    "EARLY_LOAN_SETTLE":  80,
    "CLAIM_REPORT":       40,
    "GET_QUOTE":          25,
    "BENEFICIARY_UPDATE": 35,
    "PORTFOLIO_CHECK":    20,
    "CROSS_SELL":        150,
}

# Daily limits — how many times per day each activity earns points
DAILY_LIMITS = {
    "USSD_CHECK":         1,
    "GET_QUOTE":          2,
    "POLICY_RENEWAL":     1,
    "CLAIM_REPORT":       1,
    "BENEFICIARY_UPDATE": 1,
    "PORTFOLIO_CHECK":    1,
    "UPDATE_DETAILS":     1,
}

MONTHLY_POINTS_CAP = 600  # max points a customer can earn per month

TIER_THRESHOLDS = {"Bronze": 0, "Silver": 500, "Gold": 1500, "Platinum": 3000}
TIER_ORDER      = ["Bronze", "Silver", "Gold", "Platinum"]

POLICY_TYPES = [
    "Personal Pension Plan",
    "Gadziriro Funeral Cover",
    "Security Plan",
]

CROSS_SELL_PRODUCTS = {
    "ZFS":     [("Life Assurance Policy","LIFE",50),("General Insurance","GENERAL",50),("Unit Trust Investment","ASSET",75)],
    "LIFE":    [("ZFS Loan Product","ZFS",50),("General Insurance","GENERAL",50),("Unit Trust Investment","ASSET",75)],
    "GENERAL": [("Life Assurance Policy","LIFE",50),("ZFS Loan Product","ZFS",50),("Unit Trust Investment","ASSET",75)],
    "ASSET":   [("Life Assurance Policy","LIFE",50),("ZFS Loan Product","ZFS",50),("General Insurance","GENERAL",50)],
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def hash_pin(pin):
    return hashlib.sha256(pin.encode()).hexdigest()

def generate_referral_code(name):
    prefix = name.split()[0][:3].upper()
    suffix = ''.join(random.choices(string.digits, k=4))
    return f"ZIM{prefix}{suffix}"

def calculate_tier(pts):
    t = "Bronze"
    for name, thresh in TIER_THRESHOLDS.items():
        if pts >= thresh:
            t = name
    return t


# ── Notifications ──────────────────────────────────────────────────────────────

def add_notification(customer_id, title, message, ntype="INFO"):
    conn = get_connection()
    conn.execute(
        "INSERT INTO notifications (customer_id, title, message, type) VALUES (?,?,?,?)",
        (customer_id, title, message, ntype)
    )
    conn.commit()
    conn.close()

def get_notifications(phone_number, unread_only=False):
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return []
    q = "SELECT * FROM notifications WHERE customer_id=?"
    if unread_only:
        q += " AND is_read=0"
    q += " ORDER BY created_at DESC LIMIT 30"
    rows = [dict(r) for r in conn.execute(q, (cust["id"],)).fetchall()]
    conn.close(); return rows

def mark_notifications_read(phone_number):
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if cust:
        conn.execute("UPDATE notifications SET is_read=1 WHERE customer_id=?", (cust["id"],))
        conn.commit()
    conn.close()

def unread_count(phone_number):
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return 0
    n = conn.execute(
        "SELECT COUNT(*) FROM notifications WHERE customer_id=? AND is_read=0", (cust["id"],)
    ).fetchone()[0]
    conn.close(); return n


# ── Customer ───────────────────────────────────────────────────────────────────

def register_customer(phone_number, full_name, national_id, pin, business_unit="LIFE"):
    conn = get_connection()
    cur  = conn.cursor()
    ref_code = generate_referral_code(full_name)
    try:
        cur.execute("""INSERT INTO customers
            (phone_number, full_name, national_id, pin, business_unit, referral_code)
            VALUES (?,?,?,?,?,?)""",
            (phone_number, full_name, national_id, hash_pin(pin), business_unit, ref_code))
        cust_id = cur.lastrowid
        conn.commit()
        add_notification(cust_id, "Welcome to Zimnat Rewards!",
            f"Hi {full_name.split()[0]}! Your referral code is {ref_code}. Start earning points today.", "SUCCESS")
        _seed_cross_sell(cust_id, business_unit, conn)
        return {"success": True, "message": f"Customer {full_name} registered. Referral code: {ref_code}"}
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        conn.close()

def _seed_cross_sell(customer_id, business_unit, conn):
    offers = CROSS_SELL_PRODUCTS.get(business_unit, [])
    for name, bu, pts in offers:
        conn.execute("""INSERT INTO cross_sell_offers
            (customer_id, product_name, business_unit, description, bonus_points)
            VALUES (?,?,?,?,?)""",
            (customer_id, name, bu, f"Earn {pts} bonus points when you sign up for {name}", pts))
    conn.commit()

def authenticate_customer(national_id, pin):
    """Login using National ID + PIN."""
    conn = get_connection()
    row  = conn.execute(
        "SELECT * FROM customers WHERE national_id=? AND pin=? AND is_active=1",
        (national_id, hash_pin(pin))
    ).fetchone()
    conn.close()
    if row:
        return {"success": True, "customer": dict(row)}
    return {"success": False, "message": "Invalid National ID or PIN."}

def get_customer(phone_number):
    conn = get_connection()
    row  = conn.execute("SELECT * FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_customer_by_national_id(national_id):
    conn = get_connection()
    row  = conn.execute("SELECT * FROM customers WHERE national_id=?", (national_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def update_customer_details(phone_number, full_name=None, national_id=None):
    conn = get_connection()
    if full_name:
        conn.execute("UPDATE customers SET full_name=? WHERE phone_number=?", (full_name, phone_number))
    if national_id:
        conn.execute("UPDATE customers SET national_id=? WHERE phone_number=?", (national_id, phone_number))
    conn.commit()
    conn.close()
    result = award_points(phone_number, "UPDATE_DETAILS", "Updated account details")
    return {"success": True, "message": "Details updated. " + result.get("message", "")}

def update_business_unit(phone_number, business_unit):
    conn = get_connection()
    conn.execute("UPDATE customers SET business_unit=? WHERE phone_number=?", (business_unit, phone_number))
    conn.commit()
    conn.close()
    return {"success": True, "message": f"Business unit updated to {business_unit}."}


# ── Points (with enforcement) ──────────────────────────────────────────────────

def award_points(phone_number, activity_key, description="", reference=""):
    conn = get_connection()
    points = POINTS_TABLE.get(activity_key, 0)
    if points == 0:
        conn.close()
        return {"success": False, "message": f"No points defined for activity '{activity_key}'."}

    row = conn.execute(
        "SELECT id, total_points, lifetime_points, tier FROM customers WHERE phone_number=?",
        (phone_number,)
    ).fetchone()
    if not row:
        conn.close()
        return {"success": False, "message": "Customer not found."}

    cid = row["id"]

    # ── 1. Daily limit check ───────────────────────────────────
    if activity_key in DAILY_LIMITS:
        today = date.today().isoformat()
        count = conn.execute("""SELECT COUNT(*) FROM points_transactions
            WHERE customer_id=? AND activity=? AND DATE(created_at)=?""",
            (cid, activity_key, today)).fetchone()[0]
        if count >= DAILY_LIMITS[activity_key]:
            conn.close()
            return {
                "success": False,
                "message": f"Points for '{activity_key.replace('_',' ').title()}' already earned today. Try again tomorrow!"
            }

    # ── 2. Monthly cap check ───────────────────────────────────
    this_month = date.today().strftime('%Y-%m')
    monthly_earned = conn.execute("""SELECT COALESCE(SUM(points),0) FROM points_transactions
        WHERE customer_id=? AND strftime('%Y-%m', created_at)=?""",
        (cid, this_month)).fetchone()[0]
    if monthly_earned >= MONTHLY_POINTS_CAP:
        conn.close()
        return {
            "success": False,
            "message": f"Monthly points cap of {MONTHLY_POINTS_CAP} pts reached. Resets on the 1st of next month!"
        }
    # Don't let them exceed the cap — award partial if needed
    points = min(points, MONTHLY_POINTS_CAP - monthly_earned)

    # ── 3. Award ───────────────────────────────────────────────
    new_total    = row["total_points"]    + points
    new_lifetime = row["lifetime_points"] + points
    old_tier     = row["tier"]
    new_tier     = calculate_tier(new_total)

    conn.execute(
        "UPDATE customers SET total_points=?, lifetime_points=?, tier=?, last_activity=datetime('now') WHERE id=?",
        (new_total, new_lifetime, new_tier, cid)
    )
    conn.execute(
        "INSERT INTO points_transactions (customer_id, activity, points, description, reference) VALUES (?,?,?,?,?)",
        (cid, activity_key, points, description, reference)
    )

    if new_tier != old_tier:
        conn.commit()
        add_notification(cid, f"🎉 Tier Upgrade! You are now {new_tier}!",
            f"Congratulations! You've reached {new_tier} tier. New rewards are now available.", "SUCCESS")

    conn.commit()
    conn.close()
    return {
        "success": True,
        "points_earned": points,
        "new_total": new_total,
        "tier": new_tier,
        "tier_changed": new_tier != old_tier,
        "message": f"+{points} points earned! Total: {new_total} pts. Tier: {new_tier}"
    }

def get_points_history(phone_number, limit=20):
    conn = get_connection()
    row  = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not row:
        conn.close(); return []
    rows = conn.execute("""SELECT activity, points, description, created_at FROM points_transactions
        WHERE customer_id=? ORDER BY created_at DESC LIMIT ?""", (row["id"], limit)).fetchall()
    result = [dict(r) for r in rows]
    conn.close(); return result


# ── Accounts / Policies ────────────────────────────────────────────────────────

def add_account(phone_number, account_type, account_number, policy_type,
                payment_start_date, monthly_payment_usd, monthly_payment_zig,
                currency="USD", members_covered=None):
    """
    account_number  : policy number, always starts with ZM
    policy_type     : one of POLICY_TYPES
    members_covered : list of dicts [{name, dob, relationship}]
    """
    conn = get_connection()
    row  = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not row:
        conn.close(); return {"success": False, "message": "Customer not found."}

    members_json = json.dumps(members_covered or [])
    try:
        conn.execute("""INSERT INTO accounts
            (customer_id, account_type, account_number, policy_type,
             payment_start_date, monthly_payment_usd, monthly_payment_zig,
             currency, members_covered)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (row["id"], account_type, account_number, policy_type,
             payment_start_date, monthly_payment_usd, monthly_payment_zig,
             currency, members_json))
        conn.commit()
        # Payment reminder using start date
        if payment_start_date and monthly_payment_usd > 0:
            conn.execute("""INSERT INTO payment_reminders (customer_id, account_number, amount_due, due_date)
                VALUES (?,?,?,?)""", (row["id"], account_number, monthly_payment_usd, payment_start_date))
            conn.commit()
        conn.close()
        # Award points for adding a new policy
        award_points(phone_number, "BENEFICIARY_UPDATE", f"Enrolled in {policy_type}: {account_number}")
        return {"success": True, "message": f"Policy {account_number} ({policy_type}) added successfully."}
    except Exception as e:
        conn.close(); return {"success": False, "message": str(e)}

def get_accounts(phone_number):
    conn = get_connection()
    row  = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not row:
        conn.close(); return []
    rows = conn.execute("SELECT * FROM accounts WHERE customer_id=? AND status='Active'", (row["id"],)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["members_covered"] = json.loads(d.get("members_covered") or "[]")
        except Exception:
            d["members_covered"] = []
        result.append(d)
    conn.close(); return result

def get_account_by_policy_number(policy_number):
    """Look up a single policy by its ZM... number."""
    conn = get_connection()
    row  = conn.execute("SELECT * FROM accounts WHERE account_number=?", (policy_number,)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["members_covered"] = json.loads(d.get("members_covered") or "[]")
    except Exception:
        d["members_covered"] = []
    return d

def make_payment(phone_number, account_number, amount_usd, amount_zig=0.0):
    """
    Record a premium payment. Points awarded based on timing vs payment_start_date.
    Minimum payment: $5 USD equivalent.
    """
    if amount_usd < 5.0 and amount_zig < 90.0:
        return {"success": False, "message": "Minimum payment is $5.00 USD or ZiG 90 to earn points."}

    conn = get_connection()
    acc  = conn.execute(
        "SELECT * FROM accounts WHERE account_number=? AND status='Active'", (account_number,)
    ).fetchone()
    if not acc:
        conn.close(); return {"success": False, "message": "Policy not found or inactive."}

    conn.close()

    today = date.today().isoformat()
    due   = acc["payment_start_date"] or today
    activity = "EARLY_PAYMENT" if today < due else "ON_TIME_PAYMENT"
    paid_str = f"${amount_usd:.2f} USD" + (f" / ZiG {amount_zig:.2f}" if amount_zig else "")
    desc = f"{'Early' if activity=='EARLY_PAYMENT' else 'On-time'} premium payment of {paid_str} on {account_number}"

    result = award_points(phone_number, activity, desc, account_number)
    if result["success"]:
        result["message"] = f"Payment of {paid_str} recorded. {result['message']}"
        cust = get_customer(phone_number)
        if cust:
            add_notification(cust["id"], "✅ Payment Received",
                f"{paid_str} payment on policy {account_number}. +{result['points_earned']} points earned!", "SUCCESS")
    else:
        result["message"] = f"Payment recorded but: {result['message']}"
        result["success"] = True  # payment still goes through even if points capped
    return result

def renew_policy(phone_number, account_number):
    """Renew a policy — earns points once per policy per year."""
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return {"success": False, "message": "Customer not found."}

    this_year = str(date.today().year)
    already   = conn.execute("""SELECT COUNT(*) FROM points_transactions
        WHERE customer_id=? AND activity='POLICY_RENEWAL' AND reference=?
        AND strftime('%Y', created_at)=?""",
        (cust["id"], account_number, this_year)).fetchone()[0]
    conn.close()

    if already:
        return {"success": False, "message": "This policy has already been renewed this year."}

    result = award_points(phone_number, "POLICY_RENEWAL", f"Policy renewal: {account_number}", account_number)
    if result["success"]:
        cust_full = get_customer(phone_number)
        if cust_full:
            add_notification(cust_full["id"], "🔄 Policy Renewed",
                f"Policy {account_number} renewed. +{result['points_earned']} points!", "SUCCESS")
    return result

def report_claim(phone_number, account_number, description=""):
    """Report a claim — max 3 claims per policy lifetime, earns points."""
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return {"success": False, "message": "Customer not found."}

    existing = conn.execute("""SELECT COUNT(*) FROM points_transactions
        WHERE customer_id=? AND activity='CLAIM_REPORT' AND reference=?""",
        (cust["id"], account_number)).fetchone()[0]
    conn.close()

    if existing >= 3:
        return {"success": False, "message": "Maximum of 3 claims reached for this policy."}

    desc = description or f"Claim reported on policy {account_number}"
    result = award_points(phone_number, "CLAIM_REPORT", desc, account_number)
    if result["success"]:
        cust_full = get_customer(phone_number)
        if cust_full:
            add_notification(cust_full["id"], "📋 Claim Submitted",
                f"Claim on {account_number} received. +{result['points_earned']} points for early reporting!", "INFO")
    return result

def get_quote(phone_number, product_type):
    """Get a product quote — earns points, max 2 times per day."""
    result = award_points(phone_number, "GET_QUOTE", f"Requested quote for {product_type}", product_type)
    return result


# ── Rewards ────────────────────────────────────────────────────────────────────

def get_rewards_catalogue(business_unit=None, customer_tier="Bronze"):
    conn    = get_connection()
    tier_idx = TIER_ORDER.index(customer_tier) if customer_tier in TIER_ORDER else 0
    rows    = conn.execute(
        "SELECT * FROM rewards_catalogue WHERE available=1 ORDER BY points_required"
    ).fetchall()
    result  = []
    for r in rows:
        d = dict(r)
        if d["business_unit"] not in ("ALL", business_unit or "ALL") and business_unit:
            continue
        req_idx = TIER_ORDER.index(d["tier_required"]) if d["tier_required"] in TIER_ORDER else 0
        d["tier_eligible"] = tier_idx >= req_idx
        result.append(d)
    conn.close(); return result

def redeem_reward(phone_number, reward_id):
    conn = get_connection()
    cust = conn.execute(
        "SELECT id, total_points, tier, business_unit FROM customers WHERE phone_number=?",
        (phone_number,)
    ).fetchone()
    if not cust:
        conn.close(); return {"success": False, "message": "Customer not found."}

    reward = conn.execute(
        "SELECT * FROM rewards_catalogue WHERE id=? AND available=1", (reward_id,)
    ).fetchone()
    if not reward:
        conn.close(); return {"success": False, "message": "Reward not found."}

    req_idx  = TIER_ORDER.index(reward["tier_required"]) if reward["tier_required"] in TIER_ORDER else 0
    cust_idx = TIER_ORDER.index(cust["tier"]) if cust["tier"] in TIER_ORDER else 0
    if cust_idx < req_idx:
        conn.close(); return {"success": False, "message": f"Requires {reward['tier_required']} tier or higher."}

    if cust["total_points"] < reward["points_required"]:
        conn.close(); return {"success": False,
            "message": f"Need {reward['points_required']} pts. You have {cust['total_points']}."}

    new_total = cust["total_points"] - reward["points_required"]
    new_tier  = calculate_tier(new_total)
    conn.execute("UPDATE customers SET total_points=?, tier=? WHERE id=?", (new_total, new_tier, cust["id"]))
    conn.execute("INSERT INTO redemptions (customer_id, reward_id, points_used, status) VALUES (?,?,?,'Approved')",
                 (cust["id"], reward_id, reward["points_required"]))
    if reward["stock"] > 0:
        conn.execute("UPDATE rewards_catalogue SET stock=stock-1 WHERE id=?", (reward_id,))
    conn.commit()
    conn.close()

    add_notification(cust["id"], f"🎁 Reward Redeemed: {reward['name']}",
        f"Your reward has been approved. Points used: {reward['points_required']}.", "SUCCESS")
    return {
        "success": True,
        "reward_name": reward["name"],
        "points_used": reward["points_required"],
        "remaining_points": new_total,
        "tier": new_tier,
        "message": f"'{reward['name']}' redeemed! Remaining: {new_total} pts."
    }

def get_redemption_history(phone_number):
    conn = get_connection()
    row  = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not row:
        conn.close(); return []
    rows = conn.execute("""SELECT r.name, r.reward_type, rd.points_used, rd.status, rd.redeemed_at
        FROM redemptions rd JOIN rewards_catalogue r ON r.id=rd.reward_id
        WHERE rd.customer_id=? ORDER BY rd.redeemed_at DESC""", (row["id"],)).fetchall()
    result = [dict(r) for r in rows]
    conn.close(); return result


# ── Referral ───────────────────────────────────────────────────────────────────

def refer_customer(referrer_phone, referee_phone=None, referral_code=None):
    conn     = get_connection()
    referrer = conn.execute("SELECT * FROM customers WHERE phone_number=?", (referrer_phone,)).fetchone()
    if not referrer:
        conn.close(); return {"success": False, "message": "Your account not found."}

    if referee_phone:
        referee = conn.execute("SELECT * FROM customers WHERE phone_number=?", (referee_phone,)).fetchone()
    elif referral_code:
        referee = conn.execute("SELECT * FROM customers WHERE referral_code=?", (referral_code,)).fetchone()
    else:
        conn.close(); return {"success": False, "message": "Provide referee phone or referral code."}

    if not referee:
        conn.close(); return {"success": False, "message": "Referred customer not found. They must be registered first."}
    if referee["phone_number"] == referrer_phone:
        conn.close(); return {"success": False, "message": "You cannot refer yourself."}

    existing = conn.execute(
        "SELECT id FROM referral_tracking WHERE referrer_id=? AND referee_phone=?",
        (referrer["id"], referee["phone_number"])
    ).fetchone()
    if existing:
        conn.close(); return {"success": False, "message": "You have already referred this person."}

    conn.execute("""INSERT INTO referral_tracking (referrer_id, referee_phone, referee_id, points_awarded, status)
        VALUES (?,?,?,100,'Completed')""", (referrer["id"], referee["phone_number"], referee["id"]))
    conn.commit()
    conn.close()

    result = award_points(referrer_phone, "REFERRAL", f"Referred {referee['full_name']} ({referee['phone_number']})")
    result["message"] = f"Referral successful! {result['message']}"
    return result

def get_referral_stats(phone_number):
    conn  = get_connection()
    cust  = conn.execute("SELECT * FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return {}
    total = conn.execute("SELECT COUNT(*) FROM referral_tracking WHERE referrer_id=?", (cust["id"],)).fetchone()[0]
    pts   = conn.execute("SELECT SUM(points_awarded) FROM referral_tracking WHERE referrer_id=?", (cust["id"],)).fetchone()[0] or 0
    conn.close()
    return {"total_referrals": total, "points_from_referrals": pts, "referral_code": cust["referral_code"]}


# ── Cross-sell ─────────────────────────────────────────────────────────────────

def get_cross_sell_offers(phone_number):
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return []
    rows = conn.execute(
        "SELECT * FROM cross_sell_offers WHERE customer_id=? AND status='Active' ORDER BY created_at DESC",
        (cust["id"],)
    ).fetchall()
    result = [dict(r) for r in rows]
    conn.close(); return result

def accept_cross_sell(phone_number, offer_id):
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return {"success": False, "message": "Customer not found."}
    offer = conn.execute(
        "SELECT * FROM cross_sell_offers WHERE id=? AND customer_id=?", (offer_id, cust["id"])
    ).fetchone()
    if not offer:
        conn.close(); return {"success": False, "message": "Offer not found."}
    conn.execute("UPDATE cross_sell_offers SET status='Accepted', viewed=1 WHERE id=?", (offer_id,))
    conn.commit(); conn.close()
    result = award_points(phone_number, "CROSS_SELL", f"Accepted offer: {offer['product_name']}")
    return {"success": True, "message": f"Offer accepted! {result['message']}"}


# ── Payment Reminders ──────────────────────────────────────────────────────────

def get_payment_reminders(phone_number):
    conn = get_connection()
    cust = conn.execute("SELECT id FROM customers WHERE phone_number=?", (phone_number,)).fetchone()
    if not cust:
        conn.close(); return []
    rows = conn.execute("""SELECT * FROM payment_reminders WHERE customer_id=? AND status='Pending'
        ORDER BY due_date ASC""", (cust["id"],)).fetchall()
    result = [dict(r) for r in rows]
    conn.close(); return result


# ── KPI ────────────────────────────────────────────────────────────────────────

def get_kpi_data():
    conn          = get_connection()
    total_cust    = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    active        = conn.execute("SELECT COUNT(*) FROM customers WHERE last_activity >= date('now','-30 days')").fetchone()[0]
    pts_issued    = conn.execute("SELECT SUM(points) FROM points_transactions").fetchone()[0] or 0
    pts_redeemed  = conn.execute("SELECT SUM(points_used) FROM redemptions").fetchone()[0] or 0
    total_redemptions = conn.execute("SELECT COUNT(*) FROM redemptions").fetchone()[0]
    on_time       = conn.execute("SELECT COUNT(*) FROM points_transactions WHERE activity='ON_TIME_PAYMENT'").fetchone()[0]
    early         = conn.execute("SELECT COUNT(*) FROM points_transactions WHERE activity='EARLY_PAYMENT'").fetchone()[0]
    total_payments = on_time + early
    referrals     = conn.execute("SELECT COUNT(*) FROM referral_tracking").fetchone()[0]
    cross_sells   = conn.execute("SELECT COUNT(*) FROM cross_sell_offers WHERE status='Accepted'").fetchone()[0]
    tier_breakdown = conn.execute("SELECT tier, COUNT(*) as cnt FROM customers GROUP BY tier").fetchall()
    bu_breakdown   = conn.execute("SELECT business_unit, COUNT(*) as cnt FROM customers GROUP BY business_unit").fetchall()
    monthly_pts    = conn.execute("""SELECT strftime('%Y-%m', created_at) as month, SUM(points) as pts
        FROM points_transactions GROUP BY month ORDER BY month DESC LIMIT 6""").fetchall()
    conn.close()
    return {
        "total_customers":       total_cust,
        "active_users_30d":      active,
        "engagement_rate":       round((active / total_cust * 100) if total_cust else 0, 1),
        "points_issued":         pts_issued,
        "points_redeemed":       pts_redeemed,
        "redemption_rate":       round((pts_redeemed / pts_issued * 100) if pts_issued else 0, 1),
        "total_redemptions":     total_redemptions,
        "on_time_payments":      on_time,
        "early_payments":        early,
        "total_payments":        total_payments,
        "total_referrals":       referrals,
        "cross_sell_conversions": cross_sells,
        "tier_breakdown":        {r["tier"]: r["cnt"] for r in tier_breakdown},
        "bu_breakdown":          {r["business_unit"]: r["cnt"] for r in bu_breakdown},
        "monthly_points":        [{"month": r["month"], "points": r["pts"]} for r in monthly_pts],
    }
