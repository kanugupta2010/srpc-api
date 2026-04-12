"""
generate_admin_hash.py
SRPC Enterprises Private Limited

Run this once locally to generate the ADMIN_PASSWORD_HASH for your .env file.

Usage:
    python generate_admin_hash.py

Then copy the output hash into your .env on the Droplet:
    ADMIN_PASSWORD_HASH=<paste hash here>
"""

import hashlib
import getpass

password = getpass.getpass("Enter admin password: ")
confirm  = getpass.getpass("Confirm admin password: ")

if password != confirm:
    print("Passwords do not match.")
else:
    hash_val = hashlib.sha256(password.encode()).hexdigest()
    print(f"\nAdd this to your .env on the Droplet:\n")
    print(f"ADMIN_USERNAME=admin")
    print(f"ADMIN_PASSWORD_HASH={hash_val}")