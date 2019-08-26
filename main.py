# Copyright 2018 InfAI (CC SES)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from bson.objectid import ObjectId
from flask import Flask, request
from flask_restplus import Api, Resource, fields, reqparse
from flask_cors import CORS
import json
from pymongo import MongoClient, ReturnDocument, ASCENDING, DESCENDING
from dotenv import load_dotenv
load_dotenv()

app = Flask("analytics-operator-repo")
app.config.SWAGGER_UI_DOC_EXPANSION = 'list'
CORS(app)
api = Api(app, version='0.1', title='Analytics Operator Repo API',
          description='Analytics Operator Repo API')


@api.route('/doc')
class Docs(Resource):
    def get(self):
        return api.__schema__


client = MongoClient(os.getenv('MONGO_ADDR', 'localhost'), 27017)

db = client.db

operators = db.operators

ns = api.namespace('operator', description='Operations related to operators')

o_input = api.model('Input', {
    'name': fields.String(required=True, description='Input name'),
    'type': fields.String(required=True, description='Input type'),
})

o_output = api.model('Output', {
    'name': fields.String(required=True, description='Output name'),
    'type': fields.String(required=True, description='Output type'),
})

o_config = api.model('Config', {
    'name': fields.String(required=True, description='Config name'),
    'type': fields.String(required=True, description='Config type'),
})

operator_model = api.model('Operator', {
    'name': fields.String(required=True, description='Operator name'),
    'image': fields.String(required=False, description='Name of the associated docker image'),
    'description': fields.String(required=False, description='Description of the operator'),
    'pub': fields.Boolean(required=False),
    'deploymentType': fields.String(required=False),
    'inputs': fields.List(fields.Nested(o_input)),
    'outputs': fields.List(fields.Nested(o_output)),
    'config_values': fields.List(fields.Nested(o_config))
})

operator_return = operator_model.clone('Operator', {
    '_id': fields.String(required=True, description='Flow id'),
    'userId': fields.String
})

operator_list = api.model('OperatorList', {
    "operators": fields.List(fields.Nested(operator_return))
})


@ns.route('', strict_slashes=False)
class Operator(Resource):
    @api.expect(operator_model)
    @api.marshal_with(operator_return, code=201)
    def put(self):
        """Creates a operator."""
        user_id = getUserId(request)
        req = request.get_json()
        req['userId'] = user_id
        operator_id = operators.insert_one(req).inserted_id
        o = operators.find_one({'_id': operator_id})
        print("Added operator: " + json.dumps({"_id": str(operator_id)}))
        return o, 201

    @api.marshal_with(operator_list, code=200)
    def get(self):
        """Returns a list of operators."""
        parser = reqparse.RequestParser()
        parser.add_argument('search', type=str, help='Search String', location='args')
        parser.add_argument('limit', type=int, help='Limit', location='args')
        parser.add_argument('offset', type=int, help='Offset', location='args')
        parser.add_argument('sort', type=str, help='Sort', location='args')
        args = parser.parse_args()
        limit = 0
        if not (args["limit"] is None):
            limit = args["limit"]
        offset = 0
        if not (args["offset"] is None):
            offset = args["offset"]
        if not (args["sort"] is None):
            sort = args["sort"].split(":")
        else:
            sort = ["name", "asc"]
        user_id = getUserId(request)

        if not (args["search"] is None):
            if len(args["search"]) > 0:
                ops = operators.find({'$and': [{'name': {"$regex": args["search"]}}, {'$or': [{'pub': True}, {'userId': user_id}]}]})\
                    .skip(offset).limit(limit).sort("_id", 1).sort(sort[0], ASCENDING if sort[1] == "asc" else DESCENDING)
        else:
            ops = operators.find({'$or': [{'pub': True}, {'userId': user_id}]})\
                .skip(offset).limit(limit).sort(sort[0], ASCENDING if sort[1] == "asc" else DESCENDING)

        operators_list = []
        for o in ops:
            operators_list.append(o)
        return {"operators": operators_list}


@ns.route('/<string:operator_id>', strict_slashes=False)
@api.response(404, 'Operator not found.')
class OperatorUpdate(Resource):
    @api.marshal_with(operator_return)
    def get(self, operator_id):
        """Get a operator."""
        o = operators.find_one({'_id': ObjectId(operator_id)})
        print(o)
        return o, 200

    @api.expect(operator_model)
    @api.marshal_with(operator_return)
    def post(self, operator_id):
        """Updates a operator."""
        user_id = getUserId(request)
        req = request.get_json()
        operator = operators.find_one_and_update({'$and': [{'_id': ObjectId(operator_id)}, {'userId': user_id}]}, {
            '$set': req,
        },
            return_document=ReturnDocument.AFTER)
        if operator is not None:
            return operator, 200
        return "Operator not found", 404

    @api.response(204, "Deleted")
    def delete(self, operator_id):
        """Deletes a operator."""
        user_id = getUserId(request)
        o = operators.find_one({'$and': [{'_id': ObjectId(operator_id)}, {'userId': user_id}]})
        if o is not None:
            operators.delete_one({'_id': ObjectId(operator_id)})
            return "Deleted", 204
        return "Operator not found", 404


def getUserId(req):
    user_id = req.headers.get('X-UserID')
    if user_id is None:
        user_id = os.getenv('DUMMY_USER', 'test')
    return user_id


if __name__ == "__main__":
    app.run("0.0.0.0", os.getenv('PORT', 5000), debug=False)
