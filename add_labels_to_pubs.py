import csv
import xml.etree.ElementTree as ET
import requests
from time import sleep

import program_setup


def main():
    args = program_setup.process_args()
    config = program_setup.get_config()

    # Load input file
    if not args.input_file:
        raise "Please specify an input file with -i."
    else:
        input_rows = csv.DictReader(open(args.input_file, encoding='utf-8-sig'))

    # Loop the rows; create XML bodies for API reqs
    for row in input_rows:
        print(f"\nProcessing Pub ID: {row['ID']}")
        if not row['ID'] or not row['TAG TO APPLY']:
            print("Input is missing either ID or TAGS. Skipping.")

        if args.clear_previous:
            print("Clearing existing pub object labels.")
            # Note, this will clear only pub object labels, not labels from pub records,
            # e.g. from Dimensions, or "issn-inferred" inherited from the pub's Journal
            clear_body_xml = create_clear_body_xml()
            response = send_pub_label_updates(args, config, row['ID'], clear_body_xml)
            parse_response(response)
            sleep(2)

        print("Adding new labels from input.")
        body_xml = create_body_xml(row['TAG TO APPLY'])
        print(body_xml)

        # Send the update; parse the result
        response = send_pub_label_updates(args, config, row['ID'], body_xml)
        parse_response(response)
        sleep(2)

    print('\nProgram complete. Exiting.')


# See below of example body XML.
def create_body_xml(labels):
    xml_update_object = ET.Element("update-object", attrib={'xmlns': 'http://www.symplectic.co.uk/publications/api'})
    xml_fields = ET.SubElement(xml_update_object, 'fields')
    xml_field = ET.SubElement(xml_fields, 'field', attrib={'name': 'labels', 'operation': 'add'})
    xml_keywords = ET.SubElement(xml_field, 'keywords')

    # Split the tags list and add a sub-element for each
    for label in labels.split(', '):
        xml_keyword = ET.SubElement(xml_keywords, 'keyword', attrib={'scheme': 'c-lbnl-label'})
        xml_keyword.text = label

    # Convert to string, return
    return ET.tostring(xml_update_object)


# For clearing the previous publication object labels
def create_clear_body_xml():
    xml_update_object = ET.Element("update-object", attrib={'xmlns': 'http://www.symplectic.co.uk/publications/api'})
    xml_fields = ET.SubElement(xml_update_object, 'fields')
    xml_field = ET.SubElement(xml_fields, 'field', attrib={'name': 'labels', 'operation': 'clear'})

    # Convert to string, return
    return ET.tostring(xml_update_object)


def send_pub_label_updates(args, config, elements_id, xml_body):

    # Assemble the API URL
    api_url = f"{config['ELEMENTS_API_URL_' + args.connection]}/publications/{elements_id}"
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


# Example body XML: Adding labels
#
# <update-object xmlns="http://www.symplectic.co.uk/publications/api">
#   <fields>
#     <field name="labels" operation="add">
#   	<keywords>
#     	  <keyword scheme="scheme-name">1234 Label One</keyword>
#         <keyword scheme="scheme-name">5678 Label Two</keyword>
#   	</keywords>
#     </field>
#   </fields>
# </update-object>
