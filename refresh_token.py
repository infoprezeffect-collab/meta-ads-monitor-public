"""
refresh_token.py — Renouvellement du token Meta (à faire tous les 60 jours)
Usage : python refresh_token.py
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID     = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")
OLD_TOKEN  = os.getenv("META_ACCESS_TOKEN")

print("🔄 Renouvellement du token Meta...")
print("=" * 50)

if not all([APP_ID, APP_SECRET, OLD_TOKEN]):
    print("❌ Vérifie que META_APP_ID, META_APP_SECRET et META_ACCESS_TOKEN sont dans ton .env")
    exit(1)

url = "https://graph.facebook.com/v21.0/oauth/access_token"
params = {
    "grant_type":        "fb_exchange_token",
    "client_id":         APP_ID,
    "client_secret":     APP_SECRET,
    "fb_exchange_token": OLD_TOKEN,
}

resp = requests.get(url, params=params)
data = resp.json()

if "access_token" in data:
    new_token = data["access_token"]
    expires   = data.get("expires_in", "inconnu")
    print(f"✅ Nouveau token généré !")
    print(f"⏱  Expire dans : {int(expires)//86400} jours")
    print(f"\n📋 Nouveau token :\n{new_token}")
    print("\n👉 Mets à jour META_ACCESS_TOKEN dans ton .env et dans Railway Variables")
else:
    print(f"❌ Erreur : {data}")
