from flask import Flask, render_template, json, request, jsonify, Response
import redis
import pandas as pd
from numpy import product

with open("data/20220620_frequency_bougth_together.json", "r") as f:
    product_id_btgs = json.load(f)

app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0)
df_orders = pd.read_csv("data/df_orders_full_industry_province.csv")

def get_product_id_from_redis(industry_id, province_id, product_barcode):
    product_objects = product_id_btgs[product_barcode]
    for product_object in product_objects:
        if product_object["industry_id"] == str(industry_id) and product_object["province_id"] == str(province_id):
            return product_object["list_product_id"]
    return ""

@app.route('/frequency_bought_together', methods=['GET'])
def frequency_bought_together():

    result = {}


    try:
        if request.args.get("barcode") is None:
            return ""
        else:
            product_barcode = request.args.get("barcode")
        if request.args.get("industry_id") is None:
            return ""
        else:
            industry_id = request.args.get("industry_id")
        if request.args.get("province_id") is None:
            return ""
        else:
            province_id = request.args.get("province_id")
        product_ids = get_product_id_from_redis(industry_id, province_id, product_barcode)
        results = []
        for product_id in product_ids:
            product_name = df_orders[df_orders.product_id == product_id]["product_name"].values[0]
            results.append({"product_id": product_id, "product_name": product_name})
        result["result"] = results
        data = json.dumps(result, ensure_ascii=False)
        print(data)
        return data
    except Exception as e:
        return "Exception: " + str(e)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5010)