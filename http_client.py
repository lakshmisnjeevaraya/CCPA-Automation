#############################################################
#  Purpose: Contains all the CCPA/MongoDB related Apis
#  Author: Lakshmi.Sanjeevaraya
#############################################################

import requests
import random
import string
from setup import super_logger
from setup import logger


# Get all tasks for all the groups
def get_snow_api(url, query, assignment_id, fields, custtool, user, password,
                 group, reporting_date):
    snow_message = "SNOWisnotsuccesfull"
    try:
        http_url = url + query + fields
        logger.info(f'http url:{http_url}')
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.get(http_url,
                                auth=(user, password),
                                headers=headers)
        if response.status_code == 200:
            logger.info(f'response headers:{response.headers}')
            logger.info(f'status code from SNOW API:{response.status_code}')
            data = response.json()
            logger.info(f'Snow list of task:{data}')
            return data
        else:
            logger.info(
                f'Status: {response.status_code}, Headers: {response.headers}')
            logger.info(f'ErrorResponse: {response.json()}')
            logger.error(f'Error from SNOW API is:{response}')
            logger.info(
                f'******Unable to fetch the tasks from SNOW API:*********')
            super_logger.info(f'task_id:')
            super_logger.info(f'request_id:')
            super_logger.info(f's3_file_name:')
            super_logger.info(f'execution_status:FAIL')
            super_logger.info(f'close_status_state:')
            super_logger.info(f'close_status_code:')
            super_logger.info(f'execution_date:{reporting_date}')
            super_logger.info(f'ccpa_group:{group}')
            super_logger.info(f'submission_date:')
            super_logger.info(f'error_description:',
                              "SNOW REquest is not Successfull")

    except Exception as err:
        logger.info(f'Error from SNOW occurred:{err}')
        super_logger.info(f'task_id:')
        super_logger.info(f'request_id:')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:')
        super_logger.info(f'error_description:{snow_message}')
        exit()


# Gets the consumer information from MongoDB
def get_mongo_api(uri, cpa_guid, custtool, task, user, group, reporting_date):
    interaction_id = ''.join(random.choices(string.digits, k=10))
    logger.info(f'interaction_id:{interaction_id}')
    message = 'MongoAPIisnotSuccessfull'

    try:
        v_params = {'requestId': cpa_guid}
        v_headers = {
            "accept": "application/json",
            "CUSTOMER_FACING_TOOL": custtool,
            "INTERACTION_ID": interaction_id,
            "USER_ID": user
        }

        response = requests.get(uri,
                                params=v_params,
                                headers=v_headers,
                                timeout=180)
        # If the response was successful, no Exception will be raised
        logger.info(f'Http url:{response.url}')
        status = response.status_code
        logger.info(f'Http status:{status}')
        consumerinfo = response.json()

        if response.status_code == 200:
            logger.info(f'Consumer ifno is: {consumerinfo}')

            return consumerinfo
        else:
            logger.info(
                f'Status: {response.status_code}, Headers: {response.headers}')
            logger.error(
                f'Error occurred when we hit to Mongo API to get accountID/DciD: {response.json()}'
            )
            logger.info(
                f'******Unable to fetch the consumerinfo from mongo API:{cpa_guid_task} *********'
            )
            super_logger.info(f's3_file_name:')
            super_logger.info(f'execution_status:FAIL')
            super_logger.info(f'close_status_state:')
            super_logger.info(f'close_status_code:')
            super_logger.info(f'execution_date:{reporting_date}')
            super_logger.info(f'ccpa_group:{group}')
            super_logger.info(f'submission_date:')
            super_logger.info(f'error_description:{message}')

    except Exception as err:
        logger.error(
            f'Error occurred while navigating to Mongo API to get accountID/DciD: {err}'
        )
        logger.info(
            f'******Error occured from consumerinfo mongo API for the TASK:{task} *********'
        )
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:')
        super_logger.info(f'error_description:{message}')


# Close the SNOW task
def delete_task(uri, sys_id, keyobject, user, password, state, closurecode,
                submissiondate, group, reporting_date):
    del_message = 'Deletenotsuccessfull'
    try:
        url = uri + sys_id
        logger.info(f'Delete URI:{url}')
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        data = {"state": state, "closure_code": closurecode}
        logger.info(f'Request Body:{data}')

        response = requests.patch(url,
                                  auth=(user, password),
                                  headers=headers,
                                  json=data)
        logger.info(f'Delete Response:{response.status_code}')
        close_task_status = response.status_code
        if close_task_status == 200:
            logger.info(
                f'**********Successfull Closed the task in SNoW with stats:{state} and ClosureCode: {closurecode}*********'
            )
            logger.info(
                f'******Successfull Closed the task for :{keyobject} *********'
            )
            logger.info(
                f'SNOW Task delete status where State: {state} and closurecode: {closurecode}'
            )
            super_logger.info(f'execution_status:PASS')
            super_logger.info(f'close_status_state:{state}')
            super_logger.info(f'close_status_code:{closurecode}')
            super_logger.info(f'execution_date:{reporting_date}')
            super_logger.info(f'ccpa_group:{group}')
            super_logger.info(f'submission_date:{submissiondate}')
            super_logger.info(f'error_description:')

        else:
            logger.info(
                f'Status: {close_task_status}, Headers: {close_task_status.headers}'
            )
            logger.info(f'ErrorResponse:{close_task_status.json()}')
            logger.info(
                f'******Unable to close the task for :{keyobject} *********')

            super_logger.info(f'execution_status:FAIL')
            super_logger.info(f'close_status_state:')
            super_logger.info(f'close_status_code:')
            super_logger.info(f'execution_date:{reporting_date}')
            super_logger.info(f'ccpa_group:{group}')
            super_logger.info(f'submission_date:{submissiondate}')
            super_logger.info(f'error_description:{del_message}')

    except Exception as err:
        logger.info(f'Unable to close because of:{err}')
        logger.info(
            f'******Unable to close the task for :{keyobject} *********')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{del_message}')


if __name__ == '__main__':
    get_snow_api()
# get_mongo_api()
# delete_task()
