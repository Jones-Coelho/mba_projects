from abc import ABC, abstractmethod
import boto3
import re


def get_account_id() -> str:
    account_id: str = boto3.client('sts')\
                           .get_caller_identity()\
                           .get('Account')
    return account_id


def is_s3_object_empty(bucket_name, object_key):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    try:
        # Get object metadata
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        
        # Check if object size is 0
        if response['ContentLength'] == 0:
            return True
        else:
            return False
    except s3.exceptions.NoSuchKey:
        # Object not found
        return False
    except Exception as error:
        print(f"An error occurred: {error}")
        return False


class InputBatchHandler(ABC):
    def __init__(self, file_name: str) -> None:
        self.step_function_client: boto3.client = boto3.client('stepfunctions')
        self.state_machine_arn: str = f'arn:aws:states:us-east-1:{get_account_id()}:stateMachine:{self.get_state_machine_name()}' # noqa
        self.file_name: str = file_name

    @abstractmethod
    def start_state_machine(self):
        pass

    @abstractmethod
    def get_state_machine_name(self):
        pass


class InputBatchHandlerBancos(InputBatchHandler):
    def start_state_machine(self):
        self.step_function_client.start_execution(
            stateMachineArn=self.state_machine_arn,
            input='{"file_name": "' + self.file_name + '"}'
        )
        print(f"State Machine: {self.state_machine_arn}")

    def get_state_machine_name(self):
        return 'pipeline_sor_sot_bancos'


class InputBatchHandlerReclamacoes(InputBatchHandler):
    def start_state_machine(self):
        self.step_function_client.start_execution(
            stateMachineArn=self.state_machine_arn,
            input='{"file_name": "' + self.file_name + '"}'
        )
        print(f"State Machine: {self.state_machine_arn}")

    def get_state_machine_name(self):
        return 'pipeline_sor_sot_reclamacoes'


class InputBatchHandlerEmpregados(InputBatchHandler):
    def start_state_machine(self):
        self.step_function_client.start_execution(
            stateMachineArn=self.state_machine_arn,
            input='{"file_name": "' + self.file_name + '"}')
        print(f"State Machine: {self.state_machine_arn}")

    def get_state_machine_name(self):
        return 'pipeline_sor_sot_empregados'


class InputBatchHandlerFactory:
    @staticmethod
    def create_handler(file_name: str) -> InputBatchHandler:
        if re.match(r'.*Bancos.*', file_name):
            return InputBatchHandlerBancos(file_name)
        elif re.match(r'.*Reclamacoes.*', file_name):
            return InputBatchHandlerReclamacoes(file_name)
        elif re.match(r'.*Empregados.*', file_name):
            return InputBatchHandlerEmpregados(file_name)
        else:
            raise ValueError("Unsupported file pattern")


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    filename: str = f"s3://{bucket}/{key}"
    try:
        if not is_s3_object_empty(bucket, key):
            handler: InputBatchHandler = InputBatchHandlerFactory.create_handler(filename) # noqa
            handler.start_state_machine()
    except Exception as error:
        print(error)
        print(f'Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.') # noqa
        raise error
