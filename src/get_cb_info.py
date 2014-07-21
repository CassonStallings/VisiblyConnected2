"""
get_db_info.py

Save all crunchbase info to s3 using the v1 API.
Crunchbase data retrieved through v1 API has not been updated since April.
"""

import argparse as arg
import json
import CrunchbaseApi as cb


parser = arg.ArgumentParser()
parser.add_argument('--cb_api_key', type=str, dest='cb_api_key', default='')
parser.add_argument('--aws_id', type=str, dest='aws_id', default='')
parser.add_argument('--aws_key', type=str, dest='aws_key', default='')
parser.add_argument('--log_file', type=str, dest='log_file', default='process_log.txt')

def main():
    print 'Starting Main'
    args = parser.parse_args()
    open_log_file = open(args.log_file, 'w')
    crunch = cb.CrunchbaseApi(api_key=args.cb_api_key, aws_id=args.aws_id, aws_key=args.aws_key,
                open_log_file = open_log_file)

    # Get pertinent lists to drive downloads, save in S3
    #save_all_to_s3(crunch)

    for entity_type in crunch.get_entity_types():
        if entity_type != 'products':
            print 'Getting data for list of', entity_type
            entity_list = crunch.get_entity_list_from_s3(entity_type + '.json', 'crunchbase_data', entity_type)
            singular_entity_type = crunch.get_single_entity_type(entity_type)
            dict_of_data = crunch.cycle(singular_entity_type, entity_list[0:1000])
            with open('..\data\\' + singular_entity_type + '_pages.json', 'w') as fil:
                fil.write(json.dumps(dict_of_data))
    #
    #     crunch.cycle(entity_type, entity_list[start_id:start_id+count], mongo_collection)

    open_log_file.close()
    print 'Done Cycling Through Permalinks'

def save_all_to_s3(crunch):
     for entity_type in crunch.get_entity_types():
        print 'Getting entity list for', entity_type
        crunch.put_entity_list_in_s3(key_name=entity_type + '.json',
                bucket_name='crunchbase_data', entity_type=entity_type)
     print 'All entity lists saved.'

if __name__ == '__main__':
    main()