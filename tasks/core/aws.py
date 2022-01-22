import time
from datetime import datetime
from subprocess import check_output, run

import boto3
import jmespath


class SSMOperator:
    def __init__(self, region_name='ap-northeast-2'):
        self.client = boto3.client('ssm', region_name=region_name)

    def get_parameter(self, name, with_decryption=False):
        return str(
            self.client.get_parameter(
                Name=name, WithDecryption=with_decryption
            )['Parameter']['Value']
        )

    def put_parameter(self, name, value):
        self.client.put_parameter(
            Name=name, Type='SecureString', Overwrite=True, Value=value,
        )


class ECROperator:
    """
    오류 해결이 어려울 시 DevOps / Data Engineer 에게 문의하세요.
    먼저 ECR 에 프라이빗 레포지토리가 생성되어 있어야 합니다.
    이 클래스를 사용하려면 적어도 ECR 의 레포지토리 리스트를 볼 수 있는 권한 및 레포지토리에 푸시할 수 있는 권한은 필요합니다.
    이미지를 생성하기 위한 Dockerfile 이 필요합니다.

    >>> ecr = ECROperator('data/recommend_system')
    >>> ecr.execute('/path/to/federman/recommend_system')

    이 클래스를 기본적으로 쉘 명령에서 아래 작업을 하는 것과 같습니다
    aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin account.dkr.ecr.ap-northeast-2.amazonaws.com
    docker build --no-cache -t account.dkr.ecr.ap-northeast-2.amazonaws.com/repository_name
    docker push account.dkr.ecr.ap-northeast-2.amazonaws.com/repository_name:version
    """

    def __init__(self, name, repository='237943334087.dkr.ecr.ap-northeast-2.amazonaws.com',
                 region_name='ap-northeast-2'):
        self.repository = repository
        self.name = name
        self.region_name = region_name
        self.image = f'{self.repository}/{self.name}:latest'

    def login(self, username='AWS'):
        out = check_output(['aws', 'ecr', 'get-login-password', '--region', self.region_name])
        return run(['docker', 'login', '--username', username, '--password-stdin', self.repository], input=out)

    def build(self, target, cache=False):
        command = ['docker', 'build', '-t', self.image, target]
        if not cache:
            command.insert(2, '--no-cache')
        return run(command)

    def push(self):
        return run(['docker', 'push', self.image])

    def execute(self, target):
        login = self.login()
        assert login.returncode == 0
        build = self.build(target)
        assert build.returncode == 0
        push = self.push()
        assert push.returncode == 0
        return self.image


class ECSOperator:
    """
    오류 해결이 어려울 시 DevOps / Data Engineer 에게 문의하세요.
    이 클래스를 사용하려면 적어도 ECS 의 레포지토리 작업정의를 등록할 수 있는 권한,
        작업의 상태를 볼 수 있는 권한 및 ECS 클러스터 내에서 작업을 실행할 수 있는 권한이 필요합니다.
    ECR 이미지를 사용하지 않으면 docker 에서 제공하는 python 기본 이미지를 사용합니다

    >>> ecs = ECSOperator('data-ml', 'python_test', 'python39')
    >>> ecs.execute('/path/to/federman/recommend_system')
    """

    def __init__(self, cluster, family, container_name, region_name='ap-northeast-2'):
        self.cluster = cluster
        self.family = family
        self.container_name = container_name
        self.client = boto3.client('ecs', region_name=region_name)
        self.log_client = boto3.client('logs', region_name=region_name)
        self.task_id = None
        self.error_sequence = 'ECS END'

    def get_task_log_streams(self, head=False) -> list:
        assert self.task_id is not None
        response = self.log_client.get_log_events(
            logGroupName=f'/ecs/{self.cluster}',
            logStreamName=f'ecs/{self.container_name}/{self.task_id}',
            startFromHead=head,
        )
        events: list = response['events']
        if head:
            while response['events']:
                next_token = response['nextForwardToken']
                response = self.log_client.get_log_events(
                    logGroupName=f'/ecs/{self.cluster}',
                    logStreamName=f'ecs/{self.container_name}/{self.task_id}',
                    startFromHead=head,
                    nextToken=next_token,
                )
                events.extend(response['events'])
        return events

    def check_task_app(self):
        assert self.task_id is not None
        events = self.get_task_log_streams()
        return events and events[-1].get('message', '').strip() == self.error_sequence

    def log_mirror(self):
        """print(self.error_sequence) 가 마지막에 들어있는지, 있으면 전체 로그를 리턴하고 없으면 마지막 에러 스트림만 리턴한다"""
        assert self.task_id is not None
        is_traceback = True
        head = False
        if self.check_task_app():
            head = True
            is_traceback = False

        data = '\n'
        for log in self.get_task_log_streams(head=head):
            timestamp = datetime.fromtimestamp(log['timestamp'] / 1000)
            message = log['message']
            data += f'[ECS {timestamp:%Y-%m-%d %H:%M:%S,%f}] {message}\n'
        print(data)

        # raise airflow error
        assert not is_traceback

    def register_task_definition(
            self,
            command,
            image='docker.io/python:3.9-slim-buster',
            cpu=256,
            memory=512,
            execution_role_arn='arn:aws:iam::237943334087:role/ecsTaskExecutionRole',
            entrypoint=None,
            log_group='jongwony',
    ):
        container_def = {
            'name': self.container_name,
            'image': image,
            'essential': True,
            'command': command,
            'interactive': True,
            'pseudoTerminal': True,
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': f'/ecs/{log_group}',
                    'awslogs-region': 'ap-northeast-2',
                    'awslogs-stream-prefix': 'ecs',
                },
            },
        }
        if entrypoint is not None:
            container_def['entryPoint'] = entrypoint

        return self.client.register_task_definition(
            # REQUIRED
            family=self.family,
            executionRoleArn=execution_role_arn,
            networkMode='awsvpc',
            containerDefinitions=[container_def],
            requiresCompatibilities=['FARGATE'],
            cpu=str(cpu),
            memory=str(memory),
        )

    def run_task(self, task_def=None):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task

        :param task_def: [REQUIRED]
            The family and revision (family:revision) or full ARN of the tasks definition to run.
            If a revision is not specified, the latest ACTIVE revision is used.
        :return:
        """
        if task_def is None:
            task_def = self.family

        return self.client.run_task(
            cluster=self.cluster,
            count=1,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    # REQUIRED
                    'subnets': ['subnet-2488684f'],
                    'securityGroups': ['sg-fac16695'],
                    'assignPublicIp': 'ENABLED',
                }
            },
            # REQUIRED
            taskDefinition=task_def,
        )

    def wait(self, task_arn):
        return self.client.get_waiter('tasks_stopped').wait(cluster=self.cluster, tasks=task_arn)

    def describe_tasks(self, task_arn):
        return self.client.describe_tasks(cluster=self.cluster, tasks=task_arn)

    def custom_wait(self, task_arn):
        response = self.describe_tasks(task_arn)
        last_status_set = jmespath.search('tasks[].lastStatus', response)
        while not all(status == 'STOPPED' for status in last_status_set):
            time.sleep(60)
            response = self.describe_tasks(task_arn)
            last_status_set = jmespath.search('tasks[].lastStatus', response)

    def execute(self, **kwargs):
        task_definition = kwargs['task_definition']
        response = self.register_task_definition(**task_definition)
        print(response)

        response = self.run_task()
        print(response)

        task_arn = jmespath.search('tasks[].taskArn', response)
        self.custom_wait(task_arn)

        response = self.describe_tasks(task_arn)
        print(response)

        # container error
        stop_reason = jmespath.search('tasks[].containers[].reason', response)
        print('stop_reason', stop_reason)
        if 'OutOfMemoryError: Container killed due to memory usage' in stop_reason:
            raise MemoryError(stop_reason)

        # application error logging
        for arn in task_arn:
            print('taskArn:', arn)
            self.task_id = arn.rpartition('/')[-1]
            self.log_mirror()

        results = jmespath.search('tasks[].{stopCode: stopCode, stoppedReason: stoppedReason}', response)
        print(results)

        return task_arn

