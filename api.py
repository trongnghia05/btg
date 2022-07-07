from flask import Flask, render_template, json, request, jsonify, Response
import redis
import pandas as pd
from numpy import product

with open("data/20220707_frequency_bougth_together_by_product_id.json", "r") as f:
    product_id_btgs_product_id = json.load(f)

with open("data/20220707_frequency_bougth_together_by_barcode.json", "r") as f:
    product_id_btgs_barcode = json.load(f)

app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0)
df_order = pd.read_csv("data/df_order_new.csv")
df_main_support_group_by_barcode = pd.read_csv("data/df_main_support_group_by_barcode.csv")



def get_product_id_with_product_id(industry_id, province, product_id):
    product_objects = product_id_btgs_product_id[product_id]
    for product_object in product_objects:
        if product_object["industry_id"] == str(industry_id) and product_object["province"] == province:
            return product_object["list_product_id"]
    return ""

def get_product_id_with_barcode(industry_id, province, barcode):
    product_objects = product_id_btgs_barcode[barcode]
    for product_object in product_objects:
        if product_object["industry_id"] == str(industry_id) and product_object["province"] == province:
            return product_object["list_product_id"]
    return ""


@app.route('/frequency_bought_together_with_product_id', methods=['GET'])
def frequency_bought_together_with_product_id():

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
        product_ids = get_product_id_with_product_id(industry_id, province, product_id)
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

@app.route('/frequency_bought_together_with_barcode', methods=['GET'])
def frequency_bought_together_with_barcode():

    result = {}


    try:
        if request.args.get("barcode") is None:
            return ""
        else:
            barcode = request.args.get("barcode")
        if request.args.get("industry_id") is None:
            return ""
        else:
            industry_id = request.args.get("industry_id")
        if request.args.get("industry_id") is None:
            return ""
        else:
            province = request.args.get("province")
        product_ids = get_product_id_with_barcode(industry_id, province, barcode)
        results = []
        for product_id_rc in product_ids:
            product_name = df_order[df_order.product_id == int(product_id_rc)]["product_name"].values[0]
            results.append({"product_id": int(product_id_rc), "product_name": product_name})
        df_product_name_by_barcode = df_main_support_group_by_barcode[df_main_support_group_by_barcode.product_barcode == barcode]
        df_product_name_by_barcode = df_product_name_by_barcode[(df_product_name_by_barcode.industry_id == int(industry_id)) & (df_product_name_by_barcode.province == province)]
        product_name_by_barcode = df_product_name_by_barcode.sort_values(by="sum_src", ascending=False).product_id.values[0]
        product_name_by_barcode = df_order[df_order.product_id == product_name_by_barcode].product_name.values[0]
        result["product_name"] = product_name_by_barcode
        result["products"] = results
        data = json.dumps(result, ensure_ascii=False)
        return data
    except Exception as e:
        return "Exception: " + str(e)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002)