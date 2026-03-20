from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_bcrypt import Bcrypt


db = SQLAlchemy()
login_manager = LoginManager()
bcrypt = Bcrypt()

login_manager.login_view = "auth.login"
login_manager.login_message_category = "info"


@login_manager.user_loader
def load_user(user_id):
    try:
        from app.models.user import User
        return User.query.get(int(user_id))
    except Exception:
        return None


def _register_blueprint(app, module_path, blueprint_name):
    try:
        module = __import__(module_path, fromlist=[blueprint_name])
        blueprint = getattr(module, blueprint_name)
        app.register_blueprint(blueprint)
        print(f"registered blueprint: {blueprint_name}")
    except Exception as e:
        print(f"skipped blueprint {blueprint_name}: {e}")


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    try:
        from config import Config
        app.config.from_object(Config)
    except Exception as e:
        print(f"config load warning: {e}")

    app.config.setdefault("SECRET_KEY", "dev-secret-key")
    app.config.setdefault("SQLALCHEMY_DATABASE_URI", "sqlite:///site.db")
    app.config.setdefault("SQLALCHEMY_TRACK_MODIFICATIONS", False)

    db.init_app(app)
    login_manager.init_app(app)
    bcrypt.init_app(app)

    _register_blueprint(app, "app.routes.auth", "auth")
    _register_blueprint(app, "app.routes.dashboard", "dashboard")
    _register_blueprint(app, "app.routes.manifest", "manifest")
    _register_blueprint(app, "app.routes.cancellations", "cancellations")
    _register_blueprint(app, "app.routes.monitoring", "monitoring")
    _register_blueprint(app, "app.routes.comms", "comms")
    _register_blueprint(app, "app.routes.orders", "orders")
    _register_blueprint(app, "app.routes.olivia", "olivia_bp")

    @app.context_processor
    def inject_global_nav():
        try:
            from app.services.analytics import current_operation_snapshot
            from app.services.weather import get_airlie_weather
            snap = current_operation_snapshot()
            weather = get_airlie_weather()
            return {
                "nav_weather": weather,
                "nav_counts": {
                    "orders": snap.get("today_orders", 0),
                    "messages": 0,
                    "cancelled": snap.get("cancelled_orders_30d", 0),
                },
            }
        except Exception:
            return {"nav_weather": {}, "nav_counts": {"orders": 0, "messages": 0, "cancelled": 0}}

    try:
        with app.app_context():
            db.create_all()
    except Exception as e:
        print(f"db create_all warning: {e}")

    return app
