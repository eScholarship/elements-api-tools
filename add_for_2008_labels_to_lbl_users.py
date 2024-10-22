import xml.etree.ElementTree as ET
import requests
from time import sleep
import json
import csv

import program_setup

log_file = "output/LBL-FoR-labels-updates.csv"
log_fields = ['user_id', 'Email', 'Name', 'Primary Group Descriptor',
              'removed', 'added', 'problem', 'remove_response', 'add_response']


def main():
    args = program_setup.process_args()
    config = program_setup.get_config()
    reporting_db_conn = program_setup.get_reporting_db_connection(args, config)

    # Query the reporting DB; Reformat the data nested by User ID
    new_for_data = get_user_for_codes(reporting_db_conn)

    # For spot-checking existing data
    existing_for_data = get_existing_for_data(reporting_db_conn)

    # The old data is referenced for a few spot-checks
    new_for_data = add_existing_data_to_new(new_for_data, existing_for_data)

    # Create the log CSV
    with open(log_file, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=log_fields)
        writer.writeheader()

    # main processing loop
    process_new_data_and_send(args, config, new_for_data)


def write_log_row(lr):
    with open(log_file, 'a') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=log_fields)
        writer.writerow(lr)


def process_new_data_and_send(args, config, new_data):

    problem_items = []
    no_update_items = 0

    def all_items_in_another_list(list1, list2):
        return all(item in list2 for item in list1)

    test_counter = 0
    for user_id in new_data.keys():

        # Testing breaks
        test_counter += 1
        if test_counter < 50:
            continue
        # if test_counter > 50:
        #     break

        print(f"\nProcessing User ID: {user_id}")
        log_row = {
            'user_id': user_id,
            'Email': new_data[user_id]['Email'],
            'Name': new_data[user_id]['Name'],
            'Primary Group Descriptor': new_data[user_id]['Primary Group Descriptor']}

        # Add and remove modified below
        new_item = new_data[user_id]
        new_item['remove_labels'] = []
        new_item['add_labels'] = new_item['Labels']

        # Check if the user has current tags and processes as needed
        if 'current' in new_item.keys() and new_item['current']['label_count'] != 0:

            if new_item['current']['label_count'] > 5:
                print("User currently has > 5 FoR labels")

                if all_items_in_another_list(new_item['Labels'], new_item['current']['labels']):
                    print("All new labels are present in current label set. No modifications required.")
                    print(f"New labels: {new_item['Labels']}")

                else:
                    print("Mismatch between new labels and current. Saving to problem items.")
                    print(f"Current labels: {new_item['current']['labels']}")
                    print(f"New labels: {new_item['Labels']}")
                    problem_items.append(new_item)
                    log_row['problem'] = True

                write_log_row(log_row)
                continue

            else:
                new_item['remove_labels'], new_item['add_labels'] = diff_labels(
                    new_item['current']['labels'], new_item['Labels'])

        if new_item['remove_labels'] == [] and new_item['add_labels'] == []:
            print("New labels matched current labels. No modifications required.")
            print(f"Current labels: {new_item['current']['labels']}")
            no_update_items += 1

        elif new_item['remove_labels'] != [] and new_item['add_labels'] == []:
            print("The unusual 'remove only' case: <5 existing FoR labels, "
                  "all new labels present in existing. No modification required.")
            print(f"Current labels: {new_item['current']['labels']}")
            no_update_items += 1

        else:
            print(f"Remove Labels: {new_item['remove_labels']}")
            print(f"Add Labels:    {new_item['add_labels']}")

            log_row.update({
                'added': ';'.join(new_item['add_labels']),
                'removed': ';'.join(new_item['remove_labels']) if new_item['remove_labels'] != [] else None,
                'problem': None})

            remove_body_xml = create_patch_xml(new_item['remove_labels'], "remove") \
                if new_item['remove_labels'] != [] else None

            add_body_xml = create_patch_xml(new_item['add_labels'], "add") \
                if new_item['add_labels'] != [] else None

            # Send the updates, parse the result
            if remove_body_xml:
                print("Sending Remove API Patch:")
                remove_response = send_api_patch(args, config, user_id, remove_body_xml)
                log_row['remove_response'] = remove_response.status_code
                parse_response(remove_response)

            if add_body_xml:
                print("Sending Add API Patch:")
                add_response = send_api_patch(args, config, user_id, add_body_xml)
                log_row['add_response'] = add_response.status_code
                parse_response(add_response)

        # Add the log row to output
        write_log_row(log_row)

    print("\n----------------------------------------\n")
    print(f"{no_update_items} items did not need updates.")

    if problem_items:
        print(f"{len(problem_items)} problem items. Writing to problems.json")
        with open("output/problem_items.json", 'w') as problem_file:
            json.dump(problem_items, problem_file)

    print('\nProgram complete. Exiting.')


def diff_labels(current_labels, new_labels):
    remove_labels = [c for c in current_labels if c not in new_labels]
    add_labels = [n for n in new_labels if n not in current_labels]
    return remove_labels, add_labels


def get_user_for_codes(reporting_db_conn):
    print("Querying Reporting DB for LBL users' new top 5 FoR codes.")
    sql_file = open("sql-queries/LBL-FoR-labels-new-top-5-from-pubs.sql")
    sql_query = sql_file.read()

    # Create cursor, execute query, zip into row dicts (pyodbc doesn't do this automatically)
    with reporting_db_conn.cursor() as cursor:
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    print("Converting flat list to nested structure.")
    nested = {}
    for row in rows:
        user_id = row['user_id']
        if user_id not in nested.keys():
            nested[user_id] = {'Email': row['Email'],
                               'Name': row['Name'],
                               'Primary Group Descriptor': row['Primary Group Descriptor'],
                               'Labels': [row['Label']]}

        elif len(nested[user_id]['Labels']) < 5:
            nested[user_id]['Labels'].append(row['Label'])

    return nested


def get_existing_for_data(reporting_db_conn):
    print("Querying Reporting DB for LBL users' exiting FoR Codes.")
    sql_file = open("sql-queries/LBL-FoR-labels-current.sql")
    sql_query = sql_file.read()

    # Create cursor, execute query, zip into row dicts (pyodbc doesn't do this automatically)
    with reporting_db_conn.cursor() as cursor:
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    print("Converting flat list to nested structure.")
    rows = {r['user_id']: {
        'label_count': r['label_count'],
        'labels': None if not r['labels'] else r['labels'].split(';')
    } for r in rows}

    return rows


# Adds the old FOR codes to the new data.
def add_existing_data_to_new(new_data, existing_data):
    for user_id in new_data.keys():
        if user_id in existing_data.keys():
            new_data[user_id]['current'] = existing_data[user_id]

    return new_data


# Creates XML body from an array of labels. See below for example. "add"/"remove" passed as "operation"
def create_patch_xml(labels_list, operation):
    xml_update_object = ET.Element("update-object", attrib={'xmlns': 'http://www.symplectic.co.uk/publications/api'})
    xml_fields = ET.SubElement(xml_update_object, 'fields')
    xml_field = ET.SubElement(xml_fields, 'field', attrib={'name': 'labels', 'operation': operation})
    xml_keywords = ET.SubElement(xml_field, 'keywords')

    # Add a sub-element for each label
    for label in labels_list:
        xml_keyword = ET.SubElement(xml_keywords, 'keyword', attrib={'scheme': 'for'})
        xml_keyword.text = label

    # Convert to string, return
    return ET.tostring(xml_update_object)


def send_api_patch(args, config, user_id, body_xml):
    sleep(1)  # Throttle reqs

    # Assemble the API URL
    api_url = f"{config['ELEMENTS_API_URL_' + args.connection]}/users/{user_id}"

    response = requests.patch(
        api_url,
        auth=(config['ELEMENTS_API_USERNAME_' + args.connection],
              config['ELEMENTS_API_PASSWORD_' + args.connection]),
        headers={'Content-Type': 'text/xml'},
        data=body_xml)

    return response


def parse_response(r):
    print(f"> R. Code: {r.status_code}")
    if r.status_code != 200:
        print("API PROBLEM ENCOUNTERED:")
        print(r.content)
        print()


# Stub for main
if __name__ == '__main__':
    main()


# Add labels XML Body reference
#
# <update-object xmlns="http://www.symplectic.co.uk/publications/api">
# <fields>
#     <field name="labels" operation="add">
#         <keywords>
#             <keyword scheme="for">0915 Interdisciplinary Engineering</keyword>
#             <keyword scheme="for">0202 Atomic, Molecular, Nuclear, Particle and Plasma Physics</keyword>
#             <keyword scheme="for">0299 Other Physical Sciences</keyword>
#             <keyword scheme="for">0402 Geochemistry</keyword>
#             <keyword scheme="for">0403 Geology</keyword>
#         </keywords>
#     </field>
# </fields>
# </update-object>
