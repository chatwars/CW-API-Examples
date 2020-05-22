'''
    cw-api sync example for kafka
    original file by: @sixcross
    edited by: @Aiyubus
    
    requires kafka-python = "==2.0.1":
    python -m pip install --upgrade kafka-python
'''

from json import loads

from kafka import KafkaConsumer  # 

consumer = KafkaConsumer(
    'cw2-offers',
    'cw2-deals',
    'cw2-duels',
    'cw2-sex_digest',
    'cw2-yellow_pages',
    'cw2-au_digest',

    'cw3-offers',
    'cw3-deals',
    'cw3-duels',
    'cw3-sex_digest',
    'cw3-yellow_pages',
    'cw3-au_digest',

    bootstrap_servers=['digest-api.chtwrs.com:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='some-super-duper-uniquie-id',
    value_deserializer=lambda x: loads(x.decode('utf-8')))


def handle_deals(message):
    deal = message.value

    deal_str = "New Deal:\n" + \
               f"sellerId: {deal['sellerId']}\n" + \
               f"sellerCastle: {deal['sellerCastle'],}\n" + \
               f"sellerName: {deal['sellerName']}\n" + \
               f"buyerId: {deal['buyerId']}\n" + \
               f"buyerCastle: {deal['buyerCastle']}\n" + \
               f"buyerName: {deal['buyerName']}\n" + \
               f"item: {deal['item']}\n" + \
               f"qty: {deal['qty']}\n" + \
               f"price: {deal['price']}\n"

    print(deal_str)


def handle_duels(message):
    duel = message.value

    duel_str = "New Duel:\n" + \
               "Winner:\n" + \
               f"   id: {duel['winner']['id']}\n" + \
               f"   name: {duel['winner']['name']}\n" + \
               f"   castle: {duel['winner']['castle']}\n" + \
               f"   level: {duel['winner']['level']}\n" + \
               f"   hp: {duel['winner']['hp']}\n" + \
               "Looser:\n" + \
               f"   id: {duel['loser']['id']}\n" + \
               f"   name: {duel['loser']['name']}\n" + \
               f"   castle: {duel['loser']['castle']}\n" + \
               f"   level: {duel['loser']['level']}\n" + \
               f"   hp: {duel['loser']['hp']}\n" + \
               f"isChallenge: {duel['isChallenge']}\n" + \
               f"isGuildDuel: {duel['isGuildDuel']}\n"

    print(duel_str)


def handle_offers(message):
    offer = message.value

    offer_str = 'New Offer:\n' + \
                f"sellerId: {offer['sellerId']}\n" + \
                f"sellerCastle: {offer['sellerCastle']}\n" + \
                f"sellerName: {offer['sellerName']}\n" + \
                f"item: {offer['item']}\n" + \
                f"qty: {offer['qty']}\n" + \
                f"price: {offer['price']}\n"

    print(offer_str)


def handle_sex(message):
    sex = message.value
    sex_list = ['New Cheapest Items:\n']
    for item in sex:
        item_str = f"{item['name']}: {str(item['prices'][0])}\n"
        sex_list.append(item_str)
    print(''.join(sex_list))


def handle_yellow(message):
    yellow = message.value
    yellow_list = ['Open Shops:\n']
    for shop in yellow:
        shop_str = f"{shop['kind']}{shop['name']} of {shop['ownerCastle']}\n" + \
                   f"{shop['ownerName']} - current Mana: {str(shop['mana'])} /ws_{shop['link']}\n"
        yellow_list.append(shop_str)

    print(''.join(yellow_list))


def handle_au(message):
    au = message.value
    au_list = ['Current Auctions:\n']
    for auction in au:
        auction_str = f"{auction['sellerCastle']}{auction['sellerName']} " + \
                      f"is selling {auction['itemName']} with the lot id: {auction['lotId']}. " + \
                      f"The current price is: {str(auction['price'])} \n"
        au_list.append(auction_str)

    print(''.join(au_list))


def handle_message(message):
    switcher = {
        'cw2-deals': handle_deals,
        'cw2-offers': handle_offers,
        'cw2-sex_digest': handle_sex,
        'cw2-yellow_pages': handle_yellow,
        'cw2-au_digest': handle_au,
        'cw3-deals': handle_deals,
        'cw3-offers': handle_offers,
        'cw3-sex_digest': handle_sex,
        'cw3-yellow_pages': handle_yellow,
        'cw3-au_digest': handle_au,
    }
    func = switcher.get(message.topic, lambda x: f"No handler for topic {message.topic}")
    return func(message)


for message in consumer:
    handle_message(message)
