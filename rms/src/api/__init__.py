from flask import Blueprint
from flask_restx import Api

from .auth import ns as auth_ns
from .analysis import ns as analysis_ns
from .finance import ns as finance_ns
from .hierarchy import ns as hierarchy_ns

api_blueprint = Blueprint("api", __name__)
api = Api(api_blueprint, doc=False)

api.add_namespace(auth_ns)
api.add_namespace(analysis_ns, path="/analysis")
api.add_namespace(finance_ns, path="/")
api.add_namespace(hierarchy_ns, path="/")
