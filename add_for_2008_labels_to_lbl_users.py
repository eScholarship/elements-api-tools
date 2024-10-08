import xml.etree.ElementTree as ET
import requests
from time import sleep

import program_setup


def main():
    args = program_setup.process_args()
    config = program_setup.get_config()
    reporting_db_conn = program_setup.get_reporting_db_connection(args, config)

    # Query the reporting DB; Reformat the data nested by User ID
    reporting_db_data = get_user_for_codes(reporting_db_conn)

    for user_id in reporting_db_data.keys():
        print(f"\nProcessing User ID: {user_id}")
        body_xml = create_body_xml(reporting_db_data[user_id]['Labels'])
        print(body_xml)

        # Send the update, parse the result
        response = send_pub_label_updates(args, config, user_id, body_xml)
        parse_response(response)
        sleep(1)

    print('\nProgram complete. Exiting.')


def get_user_for_codes(reporting_db_conn):
    print("Connecting to reporting DB; Querying for LBL users' top 5 FoR labels.")
    sql_file = open("sql-queries/top_5_for_2008_lbl_user_labels.sql")
    sql_query = sql_file.read()

    # Create cursor, execute query, zip into row dicts (pyodbc doesn't do this automatically)
    with reporting_db_conn.cursor() as cursor:
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    print("Converting flat list to nested structure.")
    nested = {}
    for row in rows:
        user_id = row['ID']
        if user_id not in nested.keys():
            nested[user_id] = {'Email': row['Email'],
                               'Name': row['Name'],
                               'Labels': [row['Label']]}

        elif len(nested[user_id]['Labels']) < 6:
            nested[user_id]['Labels'].append(row['Label'])

    return nested


# See below of example XML.
def create_body_xml(labels_list):
    xml_update_object = ET.Element("update-object", attrib={'xmlns': 'http://www.symplectic.co.uk/publications/api'})
    xml_fields = ET.SubElement(xml_update_object, 'fields')
    xml_field = ET.SubElement(xml_fields, 'field', attrib={'name': 'labels', 'operation': 'add'})
    xml_keywords = ET.SubElement(xml_field, 'keywords')

    # Add a sub-element for each label
    for label in labels_list:
        xml_keyword = ET.SubElement(xml_keywords, 'keyword', attrib={'scheme': 'for'})
        xml_keyword.text = label

    # Convert to string, return
    return ET.tostring(xml_update_object)


def send_pub_label_updates(args, config, user_id, xml_body):

    # Assemble the API URL
    api_url = f"{config['ELEMENTS_API_URL_' + args.connection]}/users/{user_id}"
    print(api_url)

    response = requests.patch(
        api_url,
        auth=(config['ELEMENTS_API_USERNAME_' + args.connection],
              config['ELEMENTS_API_PASSWORD_' + args.connection]),
        headers={'Content-Type': 'text/xml'},
        data=xml_body)

    return response


def parse_response(r):
    print(f"API response code: {r.status_code}")
    if r.status_code >= 300:
        print(r.content)


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
