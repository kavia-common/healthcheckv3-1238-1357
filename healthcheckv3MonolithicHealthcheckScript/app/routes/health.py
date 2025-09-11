from flask_smorest import Blueprint
from flask.views import MethodView

blp = Blueprint("Healt Check", "health check", url_prefix="/", description="Health check route")


@blp.route("/")
class HealthCheck(MethodView):
    def get(self):
        return {"message": "Healthy"}
