import requests
import pandas as pd
import csv
from datetime import datetime
import time
import random

X_range = range(-204, 203)
Y_range = range(-204, 203)

lands = []
estates = []
orders = []
buffer_size = 10000
ft_lands = True
ft_estates = True
ft_orders = True

def get_land_info(x, y):
    url = f"https://api.sandbox.game/lands/coordinates?coordinateX={x}&coordinateY={y}&includeExperience=true&includeWallet=true&includeNft=true"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return response.json()

def parse_land_info(x, y, land_info):
    land, estate, orders = None, None, []
    if land_info is None or "error" in land_info:
        land = {
            "id": None,
            "name": None,
            "description": land_info["error"] if "error" in land_info else "Land not found",
            "x": x,
            "y": y,
            "minted": False,
            "previewHash": None,
            "previewExtension": None,
            "logoHash": None,
            "logoExtension": None,
            "contentHash": None,
            "blockchainId": None,
            "sector": None,
            "url": None,
            "videoUrl": None,
            "migrated": None,
            "partnerId": None,
            "chainIdChangedAt": None,
            "createdAt": None,
            "updatedAt": None,
            "deletedAt": None,
            "ownerAddress": None,
            "ownerId": None,
            "ownerName": None,
            "ownerAvatarHash": None,
            "ownerAvatarExtension": None,
            "estate": None,
            "bundleId": None,
            "chainId": None,
            "lastIndexedAt": None,
            "neighborhoodId": None
        }
    else:
        land = {
            "id": land_info["id"],
            "name": land_info["name"],
            "description": (land_info["description"] if "description" in land_info and land_info["description"] is not None else "").replace("\n", "\\n").replace("\r", "\\r"),
            "x": x,
            "y": y,
            "minted": True,
            "previewHash": land_info["previewHash"],
            "previewExtension": land_info["previewExtension"],
            "logoHash": land_info["logoHash"],
            "logoExtension": land_info["logoExtension"],
            "contentHash": land_info["contentHash"],
            "blockchainId": land_info["blockchainId"],
            "sector": land_info["sector"],
            "url": land_info["url"],
            "videoUrl": land_info["videoUrl"],
            "migrated": land_info["migrated"],
            "partnerId": land_info["partnerId"],
            "chainIdChangedAt": land_info["chainIdChangedAt"],
            "createdAt": land_info["createdAt"],
            "updatedAt": land_info["updatedAt"],
            "deletedAt": land_info["deletedAt"],
            "ownerAddress": land_info["ownerAddress"],
            "ownerId": land_info["Wallet"]["User"]["id"] if "Wallet" in land_info and "User" in land_info["Wallet"] and land_info["Wallet"]["User"] is not None else None,
            "ownerName": land_info["Wallet"]["User"]["username"] if "Wallet" in land_info and "User" in land_info["Wallet"] and land_info["Wallet"]["User"] is not None else None,
            "ownerAvatarHash": land_info["Wallet"]["User"]["avatarHash"] if "Wallet" in land_info and "User" in land_info["Wallet"] and land_info["Wallet"]["User"] is not None else None,
            "ownerAvatarExtension": land_info["Wallet"]["User"]["avatarExtension"] if "Wallet" in land_info and "User" in land_info["Wallet"] and land_info["Wallet"]["User"] is not None else None,
            "estate": land_info["estate"],
            "bundleId": land_info["bundleId"],
            "chainId": land_info["chainId"],
            "partnerId": land_info["partnerId"],
            "lastIndexedAt": land_info["lastIndexedAt"],
            "neighborhoodId": land_info["neighborhoodId"]
        }
        if land_info["Estate"] is not None:
            estate = {
                "id": land_info["Estate"]["id"],
                "name": land_info["Estate"]["name"],
                "description": (land_info["Estate"]["description"] if "description" in land_info["Estate"] and land_info["Estate"]["description"] is not None else "").replace("\n", "\\n").replace("\r", "\\r"),
                "x": land_info["Estate"]["coordinateX"],
                "y": land_info["Estate"]["coordinateY"],
                "type": land_info["Estate"]["type"],
                "logoHash": land_info["Estate"]["logoHash"],
                "logoExtension": land_info["Estate"]["logoExtension"],
                "isComplete": land_info["Estate"]["isComplete"],
                "url": land_info["Estate"]["url"],
                "createdAt": land_info["Estate"]["createdAt"],
                "updatedAt": land_info["Estate"]["updatedAt"],
                "deletedAt": land_info["Estate"]["deletedAt"],
                "videoUrl": land_info["Estate"]["videoUrl"],
                "EstatePreviewHash": land_info["Estate"]["EstatePreviews"][0]["previewHash"] if len(land_info["Estate"]["EstatePreviews"]) > 0 else None,
                "EstatePreviewExtension": land_info["Estate"]["EstatePreviews"][0]["previewExtension"] if len(land_info["Estate"]["EstatePreviews"]) > 0 else None,
                "ownerAddress": land_info["Estate"]["ownerAddress"],
                "ownerId": land_info["Estate"]["Wallet"]["User"]["id"] if "Wallet" in land_info["Estate"] and "User" in land_info["Estate"]["Wallet"] and land_info["Estate"]["Wallet"]["User"] is not None else None,
                "ownerName": land_info["Estate"]["Wallet"]["User"]["username"] if "Wallet" in land_info["Estate"] and "User" in land_info["Estate"]["Wallet"] and land_info["Estate"]["Wallet"]["User"] is not None else None,
                "ownerAvatarHash": land_info["Estate"]["Wallet"]["User"]["avatarHash"] if "Wallet" in land_info["Estate"] and "User" in land_info["Estate"]["Wallet"] and land_info["Estate"]["Wallet"]["User"] is not None else None,
                "ownerAvatarExtension": land_info["Estate"]["Wallet"]["User"]["avatarExtension"] if "Wallet" in land_info["Estate"] and "User" in land_info["Estate"]["Wallet"] and land_info["Estate"]["Wallet"]["User"] is not None else None
            }
        if land_info["orders"] is not None and len(land_info["orders"]) > 0:
            for order in land_info["orders"]:
                orders.append({
                    "canceling": order["canceling"],
                    "endDate": order["endDate"],
                    "source": order["source"],
                    "neighborhoodId": order["neighborhoodId"],
                    "coordinateY": order["coordinateY"],
                    "coordinateX": order["coordinateX"],
                    "chainId": order["chainId"],
                    "price": order["price"],
                    "buying": order["buying"],
                    "currency": order["currency"],
                    "normalizedPrice": order["normalizedPrice"],
                    "isPremium": order["isPremium"],
                    "startDate": order["startDate"],
                    "updatedAt": order["updatedAt"],
                    "land": order["land"]["id"] if order["land"] is not None else None,
                    "landName": order["land"]["name"] if order["land"] is not None else None
                })
    return land, estate, orders

def save_lands():
    global lands, ft_lands
    if len(lands) == 0:
        return
    lands_df = pd.DataFrame(lands)
    lands_df.to_csv("files/thesandbox/lands.csv", index=False, mode="a" if not ft_lands else "w", header=ft_lands, encoding="utf-8", quoting=csv.QUOTE_ALL)
    lands = []
    ft_lands = False
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Saved {len(lands_df)} lands")

def save_estates():
    global estates, ft_estates
    if len(estates) == 0:
        return
    estates_df = pd.DataFrame(estates)
    estates_df.to_csv("files/thesandbox/estates.csv", index=False, mode="a" if not ft_estates else "w", header=ft_estates, encoding="utf-8", quoting=csv.QUOTE_ALL)
    estates = []
    ft_estates = False
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Saved {len(estates_df)} estates")

def save_orders():
    global orders, ft_orders
    if len(orders) == 0:
        return
    orders_df = pd.DataFrame(orders)
    orders_df.to_csv("files/thesandbox/orders.csv", index=False, mode="a" if not ft_orders else "w", header=ft_orders, encoding="utf-8", quoting=csv.QUOTE_ALL)
    orders = []
    ft_orders = False
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Saved {len(orders_df)} orders")

def save_land_info(land=None, estate=None, _orders=None, force=False):
    global lands, estates, orders
    if land is not None:
        lands.append(land)
    if estate is not None:
        estates.append(estate)
    if _orders is not None:
        orders.extend(_orders)
    if len(lands) >= buffer_size or force:
        save_lands()
    if len(estates) >= buffer_size or force:
        save_estates()
    if len(orders) >= buffer_size or force:
        save_orders()

def main():
    for x in X_range:
        for y in Y_range:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Processing {x}, {y}")
            land_info = get_land_info(x, y)
            land, estate, orders = parse_land_info(x, y, land_info)
            save_land_info(land, estate, orders)
            time.sleep(0.15)

def test():
    X_random_choices = random.choices(X_range, k=5)
    Y_random_choices = random.choices(Y_range, k=5)
    # X_random_choices = [-154]
    # Y_random_choices = [-13]
    for x in X_random_choices:
        for y in Y_random_choices:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Processing {x}, {y}")
            land_info = get_land_info(x, y)
            land, estate, orders = parse_land_info(x, y, land_info)
            save_land_info(land, estate, orders)
            time.sleep(0.15)
    save_land_info(force=True)

if __name__ == "__main__":
    test()




