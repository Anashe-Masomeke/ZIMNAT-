"""
app.py - Flask Web Application for Zimnat Rewards v3
Login by National ID. Policy-focused. Life Assurance primary.
"""

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os, sys, hashlib

sys.path.insert(0, os.path.dirname(__file__))
from database import init_db, get_connection
from models import (
    authenticate_customer, register_customer, get_customer, get_customer_by_national_id,
    get_accounts, get_rewards_catalogue, redeem_reward,
    get_points_history, make_payment, refer_customer,
    update_customer_details, update_business_unit, award_points, get_redemption_history,
    get_notifications, mark_notifications_read, unread_count,
    get_cross_sell_offers, accept_cross_sell, get_payment_reminders,
    get_referral_stats, get_kpi_data, renew_policy, report_claim,
    get_quote, add_notification, add_account, get_account_by_policy_number,
    POLICY_TYPES
)

app = Flask(__name__)
app.secret_key = "zimnat-rewards-v3-secret-2025"

with app.app_context():
    init_db()
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    conn.close()
    if count == 0:
        from demo import seed_demo_data
        seed_demo_data()


def require_auth():
    return "phone_number" in session

def require_admin():
    return "admin_user" in session


# ── Public ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    if require_auth(): return redirect(url_for("dashboard"))
    return render_template("login.html")

@app.route("/login", methods=["POST"])
def login():
    d = request.get_json()
    national_id = d.get("national_id", "").strip()
    pin         = d.get("pin", "").strip()
    result = authenticate_customer(national_id, pin)
    if result["success"]:
        session["phone_number"] = result["customer"]["phone_number"]
        name = result["customer"]["full_name"].split()[0]
        return jsonify({"success": True, "name": name})
    return jsonify({"success": False, "message": result["message"]})

@app.route("/register", methods=["POST"])
def register():
    d = request.get_json()
    if not all([d.get("phone_number"), d.get("full_name"), d.get("national_id"), d.get("pin")]):
        return jsonify({"success": False, "message": "All fields are required."})
    result = register_customer(
        d["phone_number"].strip(), d["full_name"].strip(),
        d["national_id"].strip(), d["pin"].strip(),
        d.get("business_unit", "LIFE").strip()
    )
    if result.get("success"):
        # Auto-login: set session so the BU picker redirect works
        session["phone_number"] = d["phone_number"].strip()
        result["name"] = d["full_name"].strip().split()[0]
    return jsonify(result)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


# ── Customer Dashboard ──────────────────────────────────────────────────────────

@app.route("/dashboard")
def dashboard():
    if not require_auth(): return redirect(url_for("index"))
    customer = get_customer(session["phone_number"])
    return render_template("dashboard.html", customer=customer)


# ── Customer API ───────────────────────────────────────────────────────────────

@app.route("/api/customer")
def api_customer():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    c = get_customer(session["phone_number"])
    return jsonify(dict(c) if c else {})

@app.route("/api/policy-types")
def api_policy_types():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(POLICY_TYPES)

@app.route("/api/update-business-unit", methods=["POST"])
def api_update_bu():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(update_business_unit(session["phone_number"], d.get("business_unit", "LIFE")))

@app.route("/api/points-history")
def api_points_history():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_points_history(session["phone_number"], limit=30))

@app.route("/api/accounts")
def api_accounts():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_accounts(session["phone_number"]))

@app.route("/api/lookup-policy", methods=["POST"])
def api_lookup_policy():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    policy_number = d.get("policy_number", "").strip()
    if not policy_number:
        return jsonify({"success": False, "message": "Policy number required."})
    acc = get_account_by_policy_number(policy_number)
    if not acc:
        return jsonify({"success": False, "message": f"Policy {policy_number} not found."})
    return jsonify({"success": True, "policy": acc})

@app.route("/api/add-account", methods=["POST"])
def api_add_account():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    required = ["account_type", "account_number", "policy_type", "payment_start_date",
                "monthly_payment_usd", "monthly_payment_zig"]
    if not all([d.get(f) for f in ["account_type", "account_number", "policy_type", "payment_start_date"]]):
        return jsonify({"success": False, "message": "Account type, policy number, policy type and payment start date are required."})
    if not d.get("account_number", "").startswith("ZM"):
        return jsonify({"success": False, "message": "Policy number must start with ZM (e.g. ZM123456)."})
    members = d.get("members_covered", [])
    return jsonify(add_account(
        session["phone_number"],
        d["account_type"],
        d["account_number"],
        d["policy_type"],
        d["payment_start_date"],
        float(d.get("monthly_payment_usd") or 0),
        float(d.get("monthly_payment_zig") or 0),
        d.get("currency", "USD"),
        members
    ))

@app.route("/api/rewards")
def api_rewards():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    c = get_customer(session["phone_number"])
    rewards = get_rewards_catalogue(c["business_unit"], c["tier"])
    return jsonify({"rewards": rewards, "points": c["total_points"], "tier": c["tier"]})

@app.route("/api/redeem", methods=["POST"])
def api_redeem():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(redeem_reward(session["phone_number"], d.get("reward_id")))

@app.route("/api/pay", methods=["POST"])
def api_pay():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(make_payment(
        session["phone_number"],
        d.get("account_number"),
        float(d.get("amount_usd") or 0),
        float(d.get("amount_zig") or 0)
    ))

@app.route("/api/renew-policy", methods=["POST"])
def api_renew():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(renew_policy(session["phone_number"], d.get("account_number")))

@app.route("/api/report-claim", methods=["POST"])
def api_claim():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(report_claim(session["phone_number"], d.get("account_number"), d.get("description", "")))

@app.route("/api/get-quote", methods=["POST"])
def api_quote():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(get_quote(session["phone_number"], d.get("product_type", "")))

@app.route("/api/refer", methods=["POST"])
def api_refer():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(refer_customer(session["phone_number"],
                                  d.get("referee_phone", "").strip() or None,
                                  d.get("referral_code", "").strip() or None))

@app.route("/api/referral-stats")
def api_referral_stats():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_referral_stats(session["phone_number"]))

@app.route("/api/redemption-history")
def api_redemption_history():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_redemption_history(session["phone_number"]))

@app.route("/api/check-points")
def api_check_points():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    result = award_points(session["phone_number"], "USSD_CHECK", "Checked account via web portal")
    c = get_customer(session["phone_number"])
    return jsonify({"customer": dict(c), "history": get_points_history(session["phone_number"], 5), "result": result})

@app.route("/api/notifications")
def api_notifications():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_notifications(session["phone_number"]))

@app.route("/api/notifications/read", methods=["POST"])
def api_notif_read():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    mark_notifications_read(session["phone_number"])
    return jsonify({"success": True})

@app.route("/api/unread-count")
def api_unread():
    if not require_auth(): return jsonify({"count": 0})
    return jsonify({"count": unread_count(session["phone_number"])})

@app.route("/api/cross-sell")
def api_cross_sell():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_cross_sell_offers(session["phone_number"]))

@app.route("/api/cross-sell/accept", methods=["POST"])
def api_cross_sell_accept():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    return jsonify(accept_cross_sell(session["phone_number"], d.get("offer_id")))

@app.route("/api/payment-reminders")
def api_reminders():
    if not require_auth(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_payment_reminders(session["phone_number"]))


# ── Admin ──────────────────────────────────────────────────────────────────────

@app.route("/admin")
def admin_page():
    if not require_admin(): return redirect(url_for("admin_login"))
    return render_template("admin_dashboard.html")

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        d        = request.get_json()
        username = d.get("username", "")
        pw       = hashlib.sha256(d.get("password", "").encode()).hexdigest()
        conn     = get_connection()
        row      = conn.execute(
            "SELECT * FROM admin_users WHERE username=? AND password=?", (username, pw)
        ).fetchone()
        conn.close()
        if row:
            session["admin_user"] = username
            return jsonify({"success": True})
        return jsonify({"success": False, "message": "Invalid credentials."})
    return render_template("admin_login.html")

@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_user", None)
    return redirect(url_for("admin_login"))
ADMIN_SETUP_KEY = "zimnat-admin-2025"  # Change this to something secret

@app.route("/admin/register", methods=["POST"])
def admin_register():
    d = request.get_json()
    if d.get("setup_key") != ADMIN_SETUP_KEY:
        return jsonify({"success": False, "message": "Invalid setup key."})
    username = d.get("username", "").strip()
    password = d.get("password", "").strip()
    full_name = d.get("full_name", "").strip()
    role = d.get("role", "admin")
    if not all([username, password, full_name]):
        return jsonify({"success": False, "message": "All fields are required."})
    pw = hashlib.sha256(password.encode()).hexdigest()
    try:
        conn = get_connection()
        conn.execute("INSERT INTO admin_users (username, password, role) VALUES (?,?,?)",
                     (username, pw, role))
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": f"Admin account '{username}' created."})
    except Exception as e:
        return jsonify({"success": False, "message": "Username already exists." if "UNIQUE" in str(e) else str(e)})
@app.route("/api/admin/kpi")
def api_admin_kpi():
    if not require_admin(): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_kpi_data())

@app.route("/api/admin/customers")
def api_admin_customers():
    if not require_admin(): return jsonify({"error": "Unauthorized"}), 401
    conn = get_connection()
    rows = conn.execute("""SELECT phone_number, full_name, national_id, business_unit, tier,
        total_points, lifetime_points, is_active, created_at, last_activity
        FROM customers ORDER BY total_points DESC""").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/admin/redemptions")
def api_admin_redemptions():
    if not require_admin(): return jsonify({"error": "Unauthorized"}), 401
    conn = get_connection()
    rows = conn.execute("""SELECT c.full_name, c.phone_number, rc.name AS reward,
        rd.points_used, rd.status, rd.redeemed_at
        FROM redemptions rd JOIN customers c ON c.id=rd.customer_id
        JOIN rewards_catalogue rc ON rc.id=rd.reward_id
        ORDER BY rd.redeemed_at DESC LIMIT 100""").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/admin/broadcast", methods=["POST"])
def api_admin_broadcast():
    if not require_admin(): return jsonify({"error": "Unauthorized"}), 401
    d        = request.get_json()
    conn     = get_connection()
    customers = conn.execute("SELECT id FROM customers WHERE is_active=1").fetchall()
    conn.close()
    for c in customers:
        add_notification(c["id"], d.get("title", "Announcement"), d.get("message", ""), d.get("type", "INFO"))
    return jsonify({"success": True, "sent_to": len(customers)})

@app.route("/api/admin/add-rewards-points", methods=["POST"])
def api_admin_bonus():
    if not require_admin(): return jsonify({"error": "Unauthorized"}), 401
    d = request.get_json()
    from models import POINTS_TABLE
    POINTS_TABLE["ADMIN_BONUS"] = int(d.get("points", 0))
    # Find customer phone by national_id or phone
    phone = d.get("phone_number", "")
    result = award_points(phone, "ADMIN_BONUS", d.get("reason", "Admin bonus"))
    POINTS_TABLE.pop("ADMIN_BONUS", None)
    return jsonify(result)


if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  ZIMNAT REWARDS v3 — Life Assurance Portal")
    print("  Customer: http://127.0.0.1:5000")
    print("  Admin:    http://127.0.0.1:5000/admin/login")
    print("  Admin login: admin / admin123")
    print("  Login with: National ID + PIN")
    print("=" * 55 + "\n")
    app.run(debug=True, port=5000)
