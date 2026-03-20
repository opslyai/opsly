from app import create_app, db, bcrypt
from app.models.user import User

app = create_app()

with app.app_context():
    db.create_all()

    email = "opsly.aip@gmail.com"
    password = "ChangeMe123!"

    existing = User.query.filter_by(email=email).first()
    if not existing:
        hashed_password = bcrypt.generate_password_hash(password).decode("utf-8")
        admin = User(email=email, password=hashed_password, is_admin=True)
        db.session.add(admin)
        db.session.commit()
        print(f"Admin user created: {email}")
        print(f"Temporary password: {password}")
    else:
        print(f"Admin already exists: {email}")
