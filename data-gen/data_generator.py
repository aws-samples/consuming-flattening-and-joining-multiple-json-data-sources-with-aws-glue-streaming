import os
import sys
import json
import time
import boto3
import random
import numpy as np
import argparse
from faker import *
import faker_commerce


def generate_products(fake, number_of_products=100):
    product_catalogue = []
    for product_number in range(1, number_of_products):
        product = {
            "sku": "product-" + str(fake.uuid4()),
            "name": fake.ecommerce_name(),
            "type": fake.ecommerce_category(),
            "price": fake.ecommerce_price()
        }

        product_catalogue.append(product)

    return product_catalogue


def generate_fake_purchase(fake, sold_products, purchase_timestamp):

    purchase = {
        "body": {
            "purchase_time": purchase_timestamp,
            "transaction_id": "T-" + str(fake.uuid4()),
            "user_id": "U-" + str(fake.uuid4()),
            "basket_items": []
        }
    }
    for product in sold_products:
        basket_item = {
            "product": {
                "sku": product["sku"],
                "name": product["name"],
                "category": product["type"],
                "pricing": {
                    "price": product["price"],
                },
                "quantity_purchased": random.randint(0, 3)
            }
        }
        purchase["body"]["basket_items"].append(basket_item)

    return purchase


def generate_prediction(purchase, percent_late, lateness, product_catalog):

    next_best_product = random.choice(product_catalog)
    late_timestamp = get_time_stamp_and_simulate_lateness_for_records(percent_late, time_delay=lateness)
    prediction = {
        "purchase_id": purchase['body'].get('transaction_id'),
        "purchase_time": purchase['body'].get('purchase_time'),
        "likelihood_score": int(np.random.choice(np.arange(1, 11), p=[0.3, 0.2, 0.1, 0.1, 0.1, 0.1, 0.04, 0.03, 0.02, 0.01])),
        "recommendation_processed_time": late_timestamp,
        "next_best_product_sku": next_best_product["sku"]
    }

    return prediction


def get_time_stamp_and_simulate_lateness_for_records(percent_late, time_delay, default_time_delay=1):

    if percent_late > 0:
        value = random.random() * 100
        if (value >= percent_late):
            return int(time.time())
        else:
            return (int(time.time()) + time_delay)
    else:
        # Default delay for the down stream processing is 1 second
        return int(time.time() + default_time_delay)

    return time.time()


def get_purchase_records(fake, product_catalog, percent_late=20, lateness=5):
    purchases = []
    predictions = []
    rate_of_records_produced_per_interval = 1
    for r in range(0, rate_of_records_produced_per_interval):

        purchase_timestamp = int(time.time())

        sold_products = random.choices(product_catalog, k=random.randint(1, 5))
        purchase = generate_fake_purchase(fake, sold_products, purchase_timestamp)
        purchase_data = json.dumps(purchase)

        purchase_json_payload = {
            'Data': bytes(purchase_data, 'utf-8'),
            'PartitionKey': 'partition_key'
        }
        purchases.append(purchase_json_payload)

        prediction_result = generate_prediction(purchase, percent_late, lateness, product_catalog)
        prediction_result_json_payload = {
            'Data': bytes(json.dumps(prediction_result), 'utf-8'),
            'PartitionKey': 'partition_key'
        }
        predictions.append(prediction_result_json_payload)

    return purchases, predictions


def send_records_to_kinesis(kinesis_client, fake, product_catalog, job_parameters, recommendation_service_delay=0.5, percent_late=1, lateness=5):

    try:
        while True:

            fake_purchases, fake_recommendations = get_purchase_records(
                fake, product_catalog, percent_late=percent_late, lateness=lateness)

            print(f"\n\n----fake purchases [{len(fake_purchases)}]----")
            print(fake_purchases)

            product_write_response = kinesis_client.put_records(
                StreamName=job_parameters.purchase_stream,
                Records=fake_purchases
            )
            print(f"----fake product_write_response [{product_write_response}]----")

            if recommendation_service_delay > 0:
                time.sleep(float(recommendation_service_delay))

            print(f"\n\n----fake recommendation [{len(fake_recommendations)}]----")
            print(fake_recommendations)

            recommendation_write_response = kinesis_client.put_records(
                StreamName=job_parameters.recommender_stream,
                Records=fake_recommendations
            )
            print(f"----fake recommendation_write_response [{recommendation_write_response}]----")
    except KeyboardInterrupt:
        print('interrupted!')

def main(args):

    # Please set the AWS_ACCESS_KEY_ID, AWS_SESSION_TOKEN and AWS_SECRET_ACCESS_KEY as local environment variables
    # in the console where you will run this script.
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    try:
        # Initialize Faker library
        faker = Faker()
        faker.add_provider(faker_commerce.Provider)

        kinesis_client = session.client('kinesis', args.region)

        # Generate 1000 different Users
        product_catalog = generate_products(faker, number_of_products=1000)

        # Create fake stream of data and send it to the two kinesis streams
        send_records_to_kinesis(kinesis_client, faker, product_catalog, args)

    except:
        print("Error:", sys.exc_info()[0])
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Faker based streaming data generator')

    parser.add_argument('--recommender_stream_name', action='store', dest='recommender_stream',
                        default='recommenderStream',
                        help='Provide Kinesis Data Stream name to recommender stream data')

    parser.add_argument('--purchase_stream_name', action='store', dest='purchase_stream',
                        default='nestedPurchaseStream',
                        help='Provide Kinesis Data Stream name to purchase stream data')

    parser.add_argument('--region', action='store', dest='region')

    args = parser.parse_args()

    main(args)
