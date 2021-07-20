import boto3
import logging
import sys
import os
from time import sleep

session = boto3.Session(profile_name='saml')
cf_client = session.client('cloudformation')
s3_client = session.client('s3')

logging.basicConfig(level=logging.INFO)
logging.getLogger('deploy')

mid_status = ('CREATE_IN_PROGRESS',
'ROLLBACK_IN_PROGRESS',
'DELETE_IN_PROGRESS',
'UPDATE_IN_PROGRESS',
'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
'UPDATE_ROLLBACK_IN_PROGRESS',
'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS')

def get_stack_exists(stack_name):
    try:
        cf_client.describe_stacks(StackName=stack_name)
        return True
    except:
        return False

def get_stack_status(stack_name):
    return cf_client.describe_stacks(StackName=stack_name)['Stacks'][0]['StackStatus']

def create_or_update_stack(stack_name, template_file_name):

    with open(template_file_name, 'r') as f:
        template_body = f.read()

    if get_stack_exists(stack_name):
        cf_client.update_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=[
                {
                    'ParameterKey': 'environmentName',
                    'ParameterValue': 'rganesan',
                },
            ])
        while True:
            stack_status = get_stack_status(stack_name)
            logging.info(stack_status)
            if stack_status == 'UPDATE_COMPLETE':
                break
            elif stack_status not in mid_status:
                raise Exception('Error while updating stack: ' + stack_name)
                exit(1)
            sleep(10)
        logging.info('Stack Updated')

    else:
        cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=[
                {
                    'ParameterKey': 'environmentName',
                    'ParameterValue': 'rganesan',
                },
            ])
        while True:
            stack_status = get_stack_status(stack_name)
            logging.info(stack_status)
            if stack_status == 'CREATE_COMPLETE':
                break
            elif stack_status not in mid_status:
                raise Exception('Error while updating stack: ' + stack_name)
                exit(1)
            sleep(10)
        logging.info('Stack Created')

if __name__ == "__main__":
    stack_name = sys.argv[1]
    template_file_name = sys.argv[2]
    create_or_update_stack(stack_name, template_file_name)

