import bcrypt

password = "password"
cost_factor = 12
prefix_version = b'2a' # Must be a bytes object

# Generate the salt with the specified cost and version
# The 'rounds' parameter is the cost factor (logarithmic work factor)
custom_salt = bcrypt.gensalt(rounds=cost_factor, prefix=prefix_version)

# Hash the password using the custom salt
password_hash = bcrypt.hashpw(password.encode('utf-8'), custom_salt).decode('utf-8')

print(f"Generated Hash: {password_hash}")