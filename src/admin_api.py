from flask import Blueprint, jsonify

import infrastructure as log

logger = log.getLogger("admin_api")

admin_api = Blueprint('admin_api', __name__)

@admin_api.route('/ping')
def ping():
    return jsonify({ "message": "pong" }), 200
