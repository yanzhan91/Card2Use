from __future__ import print_function

import boto3
import json

print('Loading function')


def get_card_info(card_id):
    card_table = boto3.resource('dynamodb').Table('Card2Use_Cards')
    return card_table.get_item(Key={'card_id': card_id})['Item']


def calc_rewards(card_info, domain, category):
    rewards = None
    if 'domains' in card_info['rewards']:
        if domain in card_info['rewards']['domains']:
            rewards = card_info['rewards']['domains'][domain]
    if rewards is None:
        if category in card_info['rewards']['categories']:
            rewards = card_info['rewards']['categories'][category]
        else:
            rewards = card_info['rewards']['categories']['ALL']

    card_info['reward'] = float(rewards)
    card_info['rewards_desc'] = list(card_info['rewards_desc'])

    del card_info['rewards']
    del card_info['card_id']

    return card_info


def handler(event, context):

    domain = event['domain']
    user_id = event['user_id']

    # Get Category
    if 'category' in event:
        category = event['category']
    else:
        domain_table = boto3.resource('dynamodb').Table('Card2Use_Domains')
        category = domain_table.get_item(Key={'domain': domain})['Item']['category']

    # Get user's cards
    user_table = boto3.resource('dynamodb').Table('Card2Use_Users')
    card_list = user_table.get_item(Key={'user_id': user_id})['Item']['card_ids']

    # Get card information
    card_list = map(lambda x: get_card_info(x), card_list)
    card_list = list(map(lambda x: calc_rewards(x, domain, category), card_list))

    return json.dumps(card_list)

