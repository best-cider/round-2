#!/usr/bin/python3
from collections import deque
import pandas as pd


def getAccountToArrayOfUsers(account, typeId):
    userToAccount = {}
    accountToUsers = {}
    for i, row in account.iterrows():
        userId = row['userid']
        typeAccountId = row[typeId]
        if typeAccountId in accountToUsers:
            accountToUsers[typeAccountId].append(userId)
        else:
            accountToUsers[typeAccountId] = [userId]

        userToAccount[userId] = typeAccountId

    return userToAccount, accountToUsers


def getOrderList(order):
    orderList = []
    for i, row in order.iterrows():
        order = row['orderid']
        buyerId = row['buyer_userid']
        sellerId = row['seller_userid']
        orderList.append({
            order: {
                'buyer_id': buyerId,
                'seller_id': sellerId
            }
        })
    return orderList


def get_user_id_and_populate_queue(user_id, seller_id, attr_dict, reverse_attr_dict, seen_attr_set, other_attr_queue_1, other_attr_queue_2):
	attr_id = attr_dict.get(user_id, None)
	if attr_id is not None:
		same_attr_id_list = reverse_attr_dict.get(attr_id, [])
		if seller_id in same_attr_id_list:
			return True
		seen_attr_set.add(user_id)
		other_attr_queue_1.extendleft([x for x in same_attr_id_list if x not in seen_attr_set])
		other_attr_queue_2.extendleft([x for x in same_attr_id_list if x not in seen_attr_set])

	return False



def is_fraud(buyer_id, seller_id, order_dict, credit_dict, reverse_credit_dict, bank_dict, reverse_bank_dict, device_dict, reverse_device_dict):

	credit_queue = deque([])
	bank_queue = deque([])
	device_queue = deque([])
	prev_command = "cc"

	seen_cc_user = set()
	seen_bank_user = set()
	seen_device_user = set()


	cc_status = get_user_id_and_populate_queue(buyer_id, seller_id, credit_dict, reverse_credit_dict, seen_cc_user, bank_queue, device_queue)
	if cc_status == True:
		return True

	bank_status = get_user_id_and_populate_queue(buyer_id, seller_id, bank_dict, reverse_bank_dict, seen_bank_user, credit_queue, device_queue)
	if bank_status == True:
		return True

	device_status = get_user_id_and_populate_queue(buyer_id, seller_id, device_dict, reverse_device_dict, seen_device_user, bank_queue, credit_queue)
	if device_status == True:
		return True

	found = False

	while found is False:
		# print(len(credit_queue), len(bank_queue), len(device_queue))
		# print(credit_queue, bank_queue, device_queue)
		# print(seen_cc_user, seen_bank_user, seen_device_user)

		if len(credit_queue) == 0 and len(bank_queue) == 0 and len(device_queue) == 0:
			break

		if prev_command == "cc":
			try:
				user_id = bank_queue.pop()
				bank_status = get_user_id_and_populate_queue(user_id, seller_id, bank_dict, reverse_bank_dict, seen_bank_user, credit_queue, device_queue)
				if bank_status == True:
					return True
			except IndexError:
				pass
			
			prev_command = "bank"

			try:
				user_id = device_queue.pop()
				device_status = get_user_id_and_populate_queue(user_id, seller_id, device_dict, reverse_device_dict, seen_device_user, bank_queue, credit_queue)
				if device_status == True:
					return True
			except IndexError:
				pass

			prev_command = "device"

		elif prev_command == "bank":

			try:
				user_id = credit_queue.pop()
				cc_status = get_user_id_and_populate_queue(user_id, seller_id, credit_dict, reverse_credit_dict, seen_cc_user, credit_queue, device_queue)
				if cc_status == True:
					return True
			except IndexError:
				pass
			
			prev_command = "cc"

			try:
				user_id = device_queue.pop()
				device_status = get_user_id_and_populate_queue(user_id, seller_id, device_dict, reverse_device_dict, seen_device_user, bank_queue, credit_queue)
				if device_status == True:
					return True
			except IndexError:
				pass

			prev_command = "device"

		elif prev_command == "device":

			try:
				user_id = credit_queue.pop()
				cc_status = get_user_id_and_populate_queue(user_id, seller_id, credit_dict, reverse_credit_dict, seen_cc_user, bank_queue, device_queue)
				if cc_status == True:
					return True
			except IndexError:
				pass

			
			prev_command = "cc"


			try:
				user_id = bank_queue.pop()
				bank_status = get_user_id_and_populate_queue(user_id, seller_id, bank_dict, reverse_bank_dict, seen_bank_user, credit_queue, device_queue)
				if bank_status == True:
					return True
			except IndexError:
				pass

			prev_command = "bank"

	return found


bankAccounts = pd.read_csv('bank_accounts.csv')
credit = pd.read_csv('credit_cards.csv')
device = pd.read_csv('devices.csv')
order = pd.read_csv('orders.csv')


userToBank, bankToArrayOfUsers = getAccountToArrayOfUsers(
    bankAccounts, 'bank_account')
userToCredit, creditToArrayOfUsers = getAccountToArrayOfUsers(
    credit, 'credit_card')
userToDevice, deviceToArrayOfUsers = getAccountToArrayOfUsers(device, 'device')
orderList = getOrderList(order)

for order_dict in orderList:
	for order_id, order_i in order_dict.items():
		# print(order_i)
		print(order_id, is_fraud(order_i['buyer_id'], order_i['seller_id'], order_i, userToCredit, creditToArrayOfUsers, userToBank, bankToArrayOfUsers, userToDevice, deviceToArrayOfUsers))



###### Tests

# direct_order_dict = {"123": {"buyer_id": "1", "seller_id": "2"}}
# direct_credit_dict = {"1": "abc", "2": "abc"}
# direct_reverse_credit_dict = {"abc": ["1", "2"]}

# print(is_fraud("1", "2", direct_order_dict, direct_credit_dict, direct_reverse_credit_dict, {}, {}, {}, {}))

# indirect_order_dict = {"123": {"buyer_id": "1", "seller_id": "3"}}
# indirect_credit_dict = {"1": "abc", "2": "abc"}
# indirect_reverse_credit_dict = {"abc": ["1", "2"]}
# indirect_bank_dict = {"2": "def", "3": "def"}
# indirect_reverse_bank_dict = {"def": ["2", "3"]}
# indirect_device_dict = {}
# indirect_reverse_device_dict = {}

# print(is_fraud("1", "2", indirect_order_dict, indirect_credit_dict, indirect_reverse_credit_dict, indirect_bank_dict, indirect_reverse_bank_dict, indirect_device_dict, indirect_reverse_device_dict))

# direct_order_dict = {"123": {"buyer_id": "1", "seller_id": "2"}}
# direct_credit_dict = {"1": "abc"}
# direct_reverse_credit_dict = {"abc": ["1"]}
# direct_bank_dict = {"1": "aaa", "2": "bbb"}
# direct_reverse_bank_dict = {"aaa": ["1"], "bbb": ["2"]}
# direct_device_dict = {"1": "zzz", "2": "yyy"}
# direct_reverse_device_dict = {"zzz": ["1"], "yyy": ["2"]}

# print(is_fraud("1", "2", direct_order_dict, direct_credit_dict, direct_reverse_credit_dict, direct_bank_dict, direct_reverse_bank_dict, direct_device_dict, direct_reverse_device_dict))





# direct_order_dict = {"123": {"buyer_id": "1", "seller_id": "2"}}
# direct_credit_dict = {"1": "abc", "2": "abc"}
# direct_reverse_credit_dict = {"abc": ["1", "2"]}

# print(is_fraud("1", "2", {}, {}, {}, {}, {}, {}, {}))









