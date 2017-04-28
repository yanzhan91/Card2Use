from __future__ import print_function
from boto3.dynamodb.conditions import Key

import boto3


def handler(event, context):

    print(event)

    try:
        user_id = event['user_id']
        card_name = event['card_name']
    except KeyError:
        return generate_response('400', 'User id or card name not specified')

    try:
        card_id = get_card_id_from_name(card_name)

        user_table = boto3.resource('dynamodb').Table('Card2Use_Users')
        response = user_table.update_item(
            Key={
                'user_id': user_id
            },
            UpdateExpression='add card_ids :c',
            ExpressionAttributeValues={
                ':c': {card_id}
            }
        )
        print(response)

        return generate_response(response['ResponseMetadata']['HTTPStatusCode'], 'No Error')

    except Exception as e:
        print(e)
        generate_response(500, 'Exception Thrown')


def get_card_id_from_name(card_name):
    card_table = boto3.resource('dynamodb').Table('Card2Use_Cards')
    card_items = card_table.query(
        IndexName='card_name-index',
        KeyConditionExpression=Key('card_name').eq(card_name),
        Limit=1
    )['Items']
    if not card_items:
        raise Exception
    return card_items[0]['card_id']


def generate_response(code, message):
    response = {
        'status_code': code,
        'message': message
    }
    print(response)
    return response


if __name__ == "__main__":
    mock_event = {
        'user_id': '10001',
        'card_name': 'Amazon Prime Rewards',
    }
    handler(mock_event, None)
