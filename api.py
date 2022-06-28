from flask import Flask, render_template, json, request, jsonify, Response
import redis
import pandas as pd
from numpy import product

with open("data/btg_tinh_theo_product_id.json", "r") as f:
    product_id_btgs = json.load(f)

app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0)
df_order = pd.read_csv("data/df_order_new.csv")

def get_product_id_from_redis(industry_id, province, product_id):
    product_objects = product_id_btgs[product_id]
    for product_object in product_objects:
        if product_object["industry_id"] == str(industry_id) and product_object["province"] == province:
            return product_object["list_product_id"]
    return ""

@app.route('/frequency_bought_together', methods=['GET'])
def frequency_bought_together():

    result = {}


    try:
        if request.args.get("product_id") is None:
            return ""
        else:
            product_id = request.args.get("product_id")
        if request.args.get("industry_id") is None:
            return ""
        else:
            industry_id = request.args.get("industry_id")
        if request.args.get("industry_id") is None:
            return ""
        else:
            province = request.args.get("province")
        product_ids = get_product_id_from_redis(industry_id, province, product_id)
        results = []
        for product_id_rc in product_ids:
            product_name = df_order[df_order.product_id == int(product_id_rc)]["product_name"].values[0]
            results.append({"product_id": int(product_id_rc), "product_name": product_name})

        result["product_name"] = df_order[df_order.product_id == int(product_id)].product_name.values[0]
        result["products"] = results
        data = json.dumps(result, ensure_ascii=False)
        return data
    except Exception as e:
        return "Exception: " + str(e)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002)