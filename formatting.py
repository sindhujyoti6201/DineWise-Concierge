import json

# Your restaurant data
data = [{'RestaurantID': 'Yg86Kb9NduQoUrQHreqdDg', 'Cuisine': 'Indian'}, {'RestaurantID': 'tvD0keymUc7ndbTod3R28w', 'Cuisine': 'Indian'}, {'RestaurantID': 'cJfbydHniqWsGL2VHVDHEg', 'Cuisine': 'Indian'}, {'RestaurantID': 'dETpJUa-Dz4DsavSH7a7Gg', 'Cuisine': 'Indian'}, {'RestaurantID': 'JCKCwBJmdOX_JJ0J0h_6pQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'wXp__hdga76Lkc6vX_O8dQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'bzr-TaNYNTPUMeP7T-RQuQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'VC1udoc_sbHdFaBr0-bMsA', 'Cuisine': 'Indian'}, {'RestaurantID': 'ZsIHA3Dsrdhn1bjwTxJwag', 'Cuisine': 'Indian'}, {'RestaurantID': 'uZHp2v_Vh_xTQhi4T1V81Q', 'Cuisine': 'Indian'}, {'RestaurantID': 'AMOEv303Bak1CXSbdRhZmg', 'Cuisine': 'Indian'}, {'RestaurantID': 'COhnRLIOjM1OOk8FhG8fug', 'Cuisine': 'Indian'}, {'RestaurantID': 'hFnZLTDy-nEJmpamEHZfLQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'Z4Jn4F41a-S1V7LCwyJM8Q', 'Cuisine': 'Indian'}, {'RestaurantID': 'vD1iE9QvBYCeiBuz0j9K2A', 'Cuisine': 'Indian'}, {'RestaurantID': 'UmaTRCXEKePqeQNW6VDg8w', 'Cuisine': 'Indian'}, {'RestaurantID': '1xaasQgyqyhzlsWhVVRVVg', 'Cuisine': 'Indian'}, {'RestaurantID': 'FXDMFBM-rV2s5MHzA33d2g', 'Cuisine': 'Indian'}, {'RestaurantID': 'PJOixdLWFkWNWKCAQX3jsQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'Qv9uCarXHARRCmMvXFzalg', 'Cuisine': 'Indian'}, {'RestaurantID': 'aroY6t0It9EYWCRL8u7iYg', 'Cuisine': 'Indian'}, {'RestaurantID': '_h-CtJVL9EPzF6Fxn46xQA', 'Cuisine': 'Indian'}, {'RestaurantID': 'dc3QDT31XmMF5EU1HmrKZg', 'Cuisine': 'Indian'}, {'RestaurantID': 'V0ZWJG0pH2HmA0yPiSKiHQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'edcwzFKHIDpvS7Rj9gvU6Q', 'Cuisine': 'Indian'}, {'RestaurantID': 'r_jnIM3ziVhCUKMI3iQ6WQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'jinXFLvOqN3QVe3gL30U_A', 'Cuisine': 'Indian'}, {'RestaurantID': '2Ilqvf7qs2lpHEjOF9yV4A', 'Cuisine': 'Indian'}, {'RestaurantID': 'iLjrQvbUYPj6NLkalyau0w', 'Cuisine': 'Indian'}, {'RestaurantID': 'sujhjub0fk9HmUtO_tM0Ng', 'Cuisine': 'Indian'}, {'RestaurantID': '9mcI2GEnyS3xMyHigh0Qjg', 'Cuisine': 'Indian'}, {'RestaurantID': '68qqd0gi46qcMMDt9mbEAA', 'Cuisine': 'Indian'}, {'RestaurantID': 'zJjc_IhwP-o_kRQcb0ZVYg', 'Cuisine': 'Indian'}, {'RestaurantID': 'Rk6TXh7nINbzJoGtI2C5cA', 'Cuisine': 'Indian'}, {'RestaurantID': 'lrwkR8IVohWQM551BUYcZQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'juAQ4DHCW_24LpV1T1JmBA', 'Cuisine': 'Indian'}, {'RestaurantID': 'SI4LOqIHQEgYjG_2aaDm3A', 'Cuisine': 'Indian'}, {'RestaurantID': 'KxWyFrX2JLybTFhLdFMTnw', 'Cuisine': 'Indian'}, {'RestaurantID': '1EISJscjVp3jFOjE_1BnOw', 'Cuisine': 'Indian'}, {'RestaurantID': '7VIUu8ay754GoUYEpsmxOA', 'Cuisine': 'Indian'}, {'RestaurantID': 'brNwUVFiniCGlW7GJ2klmw', 'Cuisine': 'Indian'}, {'RestaurantID': 'OHel-rETQc5EAw-qh6VU2w', 'Cuisine': 'Indian'}, {'RestaurantID': 'jdfO0RGI1Wspn6sCR3OY7Q', 'Cuisine': 'Indian'}, {'RestaurantID': 'dKbk0C9z_mRXh24-0vIHqA', 'Cuisine': 'Indian'}, {'RestaurantID': 'iXrKGRzIZK0pp31mO3nBzA', 'Cuisine': 'Indian'}, {'RestaurantID': 'A-ert0jDRBfku9ogyW_mEg', 'Cuisine': 'Indian'}, {'RestaurantID': 'CNs1LYGNTGyiGFB3QubhLQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'F1nqQsNrJpw0qk1ZFaLTOQ', 'Cuisine': 'Indian'}, {'RestaurantID': 'dC_kA-R2qb-0iEmoI4VKPg', 'Cuisine': 'Indian'}, {'RestaurantID': 'kPE1RinxVyPbPmdGu9R85A', 'Cuisine': 'Indian'}]



def format_bulk_data(data):
    bulk_data = []
    for restaurant in data:
        # First line: the action metadata (index)
        bulk_data.append(json.dumps({
            "index": {
                "_index": "restaurants",
                "_id": restaurant['RestaurantID']
            }
        }))
        # Second line: the actual document
        bulk_data.append(json.dumps({
            "RestaurantID": restaurant['RestaurantID'],
            "Cuisine": restaurant['Cuisine']
        }))
    return "\n".join(bulk_data)

# Generate the bulk formatted data
bulk_request = format_bulk_data(data)

# Save to a file
with open('bulk_request3.json', 'w') as f:
    f.write(bulk_request)

print("Bulk request formatted and saved to 'bulk_request.json'")