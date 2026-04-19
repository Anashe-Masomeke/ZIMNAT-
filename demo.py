"""demo.py — seeds DB with sample Life Assurance data (v3)"""
import os, json
from database import init_db
from models import register_customer, add_account, award_points, refer_customer

def seed_demo_data():
    customers = [
        ("0771111111", "Alice Moyo",   "63-111111A01", "1234", "LIFE"),
        ("0772222222", "Bob Ncube",    "63-222222B02", "5678", "LIFE"),
        ("0773333333", "Carol Dube",   "63-333333C03", "9999", "GENERAL"),
        ("0774444444", "David Banda",  "63-444444D04", "4321", "ASSET"),
        ("0775555555", "Eve Mutasa",   "63-555555E05", "1111", "ZFS"),
    ]
    for c in customers:
        register_customer(*c)

    # Alice — LIFE, two policies
    add_account("0771111111", "LIFE", "ZM100001", "Personal Pension Plan",
                "2025-01-15", 50.00, 3600.00, "USD",
                [{"name": "Alice Moyo", "dob": "1985-03-10", "relationship": "Main Member"},
                 {"name": "John Moyo",  "dob": "1983-07-22", "relationship": "Spouse"},
                 {"name": "Lisa Moyo",  "dob": "2010-05-01", "relationship": "Child"}])

    add_account("0771111111", "LIFE", "ZM100002", "Gadziriro Funeral Cover",
                "2025-02-01", 20.00, 1440.00, "USD",
                [{"name": "Alice Moyo",    "dob": "1985-03-10", "relationship": "Main Member"},
                 {"name": "Anna Moyo",     "dob": "1955-11-30", "relationship": "Mother"}])

    # Bob — LIFE
    add_account("0772222222", "LIFE", "ZM200001", "Security Plan",
                "2025-01-10", 80.00, 5760.00, "USD",
                [{"name": "Bob Ncube",   "dob": "1979-06-15", "relationship": "Main Member"},
                 {"name": "Grace Ncube", "dob": "1981-02-28", "relationship": "Spouse"}])

    # Carol — GENERAL
    add_account("0773333333", "GENERAL", "ZM300001", "Security Plan",
                "2025-03-01", 30.00, 2160.00, "USD",
                [{"name": "Carol Dube", "dob": "1990-09-18", "relationship": "Main Member"}])

    # David — ASSET
    add_account("0774444444", "ASSET", "ZM400001", "Personal Pension Plan",
                "2025-01-01", 200.00, 14400.00, "USD",
                [{"name": "David Banda", "dob": "1975-12-05", "relationship": "Main Member"}])

    # Eve — ZFS
    add_account("0775555555", "ZFS", "ZM500001", "Gadziriro Funeral Cover",
                "2025-04-01", 15.00, 1080.00, "USD",
                [{"name": "Eve Mutasa",  "dob": "1992-08-14", "relationship": "Main Member"},
                 {"name": "Tom Mutasa",  "dob": "1991-01-20", "relationship": "Spouse"}])

    # Award some historical points
    activities = [
        ("0771111111", "ON_TIME_PAYMENT",  "Jan premium payment"),
        ("0771111111", "POLICY_RENEWAL",   "Annual renewal"),
        ("0772222222", "ON_TIME_PAYMENT",  "Jan premium payment"),
        ("0772222222", "BENEFICIARY_UPDATE","Added beneficiary"),
        ("0773333333", "CLAIM_REPORT",     "Early claim report"),
        ("0773333333", "GET_QUOTE",        "Travel insurance quote"),
        ("0774444444", "REGULAR_INVEST",   "Monthly investment"),
        ("0774444444", "PORTFOLIO_CHECK",  "Portfolio review"),
        ("0775555555", "REFERRAL",         "Referred friend"),
        ("0775555555", "EARLY_PAYMENT",    "Early premium settlement"),
    ]
    for phone, act, desc in activities:
        award_points(phone, act, desc)

    refer_customer("0771111111", referee_phone="0775555555")

    print("[SEED] Demo data ready (v3):")
    print("  Login with National ID + PIN:")
    print("  63-111111A01 / 1234  (Alice Moyo — LIFE)")
    print("  63-222222B02 / 5678  (Bob Ncube — LIFE)")
    print("  63-333333C03 / 9999  (Carol Dube — GENERAL)")
    print("  Admin: /admin/login  user=admin  pass=admin123")

if __name__ == "__main__":
    if os.path.exists("zimnat_rewards.db"):
        os.remove("zimnat_rewards.db")
    init_db()
    seed_demo_data()
    print("Done.")
